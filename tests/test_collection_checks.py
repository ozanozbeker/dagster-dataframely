"""The asset checks `dd.collection_checks` returns for a Dataframely Collection, and what a run of them reports."""

from pathlib import Path
from typing import Annotated

import dagster as dg
import dataframely as dy
import polars as pl
import pytest

import dagster_dataframely as dd
from dagster_dataframely.errors import (
    FailurePropagationError,
    InvalidMembersError,
    InvalidSeverityError,
)
from tests.scenario import records, storage


class Customers(dy.Schema):
    customer_id = dy.Int64(primary_key=True)
    name = dy.String()


class Orders(dy.Schema):
    order_id = dy.Int64(primary_key=True)
    customer_id = dy.Int64()


class Shop(dy.Collection):
    """A foreign key: `orders.customer_id` references `customers.customer_id`."""

    orders: dy.LazyFrame[Orders]
    customers: Annotated[
        dy.LazyFrame[Customers], dy.CollectionMember(ignored_in_filters=True)
    ]

    @dy.filter()
    def order_references_customer(self) -> pl.LazyFrame:
        """Every order's customer exists."""
        return self.orders.join(self.customers, on="customer_id", how="semi")


class Invoices(dy.Schema):
    invoice_id = dy.Int64(primary_key=True)


class InvoiceLines(dy.Schema):
    invoice_id = dy.Int64(primary_key=True)
    line = dy.Int64(primary_key=True)


class InvoiceBook(dy.Collection):
    """A parent and its children, which share the primary key `invoice_id`."""

    # Unannotated, because Dataframely raises for a Collection annotation that is not a frame.
    __dagster_dataframely__ = {"severity": {"invoice_has_lines": "warn"}}  # noqa: RUF012

    invoices: dy.DataFrame[Invoices]
    lines: dy.DataFrame[InvoiceLines]

    @dy.filter()
    def invoice_has_lines(self) -> pl.LazyFrame:
        return self.invoices.join(self.lines, on="invoice_id", how="semi").lazy()


class Promos(dy.Schema):
    customer_id = dy.Int64(primary_key=True)


class ShopWithPromos(dy.Collection):
    """`Shop` plus an optional member."""

    orders: dy.LazyFrame[Orders]
    customers: Annotated[
        dy.LazyFrame[Customers], dy.CollectionMember(ignored_in_filters=True)
    ]
    promos: Annotated[
        dy.LazyFrame[Promos] | None, dy.CollectionMember(ignored_in_filters=True)
    ]

    @dy.filter()
    def order_references_customer(self) -> pl.LazyFrame:
        return self.orders.join(self.customers, on="customer_id", how="semi")


ORDERS = dg.AssetSpec("orders")
CUSTOMERS = dg.AssetSpec("customers")
INVOICE_MEMBERS = {
    "invoices": dg.AssetSpec("invoices"),
    "lines": dg.AssetSpec("invoice_lines"),
}


def shop_assets(orders_rows: dict[str, list[int]]) -> list[dg.AssetsDefinition]:
    """Return the two member assets of `Shop`, with the orders given, and their checks."""

    @dd.asset(Customers)
    def customers() -> pl.DataFrame:
        return pl.DataFrame({"customer_id": [1, 2], "name": ["Ada", "Bo"]})

    @dd.asset(Orders)
    def orders() -> pl.DataFrame:
        return pl.DataFrame(orders_rows)

    return [
        customers,
        orders,
        dd.collection_checks(Shop, members={"orders": orders, "customers": customers}),
    ]


def evaluations(
    result: dg.ExecuteInProcessResult,
) -> dict[dg.AssetCheckKey, dg.AssetCheckEvaluation]:
    """Index the run's collection filter check evaluations by check key."""
    return {
        evaluation.asset_check_key: evaluation
        for evaluation in result.get_asset_check_evaluations()
        if evaluation.check_name.startswith("dy_filter__")
    }


ORDERS_CHECK = dg.AssetCheckKey(
    dg.AssetKey("orders"), "dy_filter__order_references_customer"
)


class TestDefinition:
    """The definition `dd.collection_checks` returns."""

    def test_a_foreign_key_has_one_check_on_the_referencing_member(self):
        checks = dd.collection_checks(
            Shop, members={"orders": ORDERS, "customers": CUSTOMERS}
        )

        [spec] = checks.check_specs
        assert spec.key == ORDERS_CHECK
        assert {dep.asset_key for dep in spec.additional_deps} == {
            dg.AssetKey("customers")
        }
        assert spec.blocking
        assert spec.description == "Every order's customer exists."

    def test_members_that_share_a_primary_key_each_have_a_check(self):
        checks = dd.collection_checks(
            InvoiceBook,
            members=INVOICE_MEMBERS,
        )

        assert {
            spec.key: {dep.asset_key for dep in spec.additional_deps}
            for spec in checks.check_specs
        } == {
            dg.AssetCheckKey(dg.AssetKey("invoices"), "dy_filter__invoice_has_lines"): {
                dg.AssetKey("invoice_lines")
            },
            dg.AssetCheckKey(
                dg.AssetKey("invoice_lines"), "dy_filter__invoice_has_lines"
            ): {dg.AssetKey("invoices")},
        }

    def test_a_warn_collection_filter_has_a_non_blocking_check(self):
        checks = dd.collection_checks(
            InvoiceBook,
            members=INVOICE_MEMBERS,
        )

        assert not any(spec.blocking for spec in checks.check_specs)

    def test_an_undocumented_collection_filter_is_described_by_its_name(self):
        checks = dd.collection_checks(
            InvoiceBook,
            members=INVOICE_MEMBERS,
        )

        assert {spec.description for spec in checks.check_specs} == {
            "invoice_has_lines"
        }

    def test_a_lazy_member_is_an_input_typed_as_a_lazy_frame(self):
        checks = dd.collection_checks(
            Shop, members={"orders": ORDERS, "customers": CUSTOMERS}
        )

        assert {
            input_def.name: input_def.dagster_type.typing_type
            for input_def in checks.node_def.input_defs
        } == {"orders": pl.LazyFrame, "customers": pl.LazyFrame}


class TestCallErrors:
    """The errors `dd.collection_checks` raises when it is called."""

    def test_a_missing_required_member_raises(self):
        with pytest.raises(InvalidMembersError) as raised:
            dd.collection_checks(Shop, members={"orders": ORDERS})

        assert str(raised.value) == (
            "`members` lacks the required member 'customers' of Shop. "
            "The members of Shop are 'orders', 'customers'."
        )

    def test_an_unknown_member_name_raises(self):
        with pytest.raises(InvalidMembersError) as raised:
            dd.collection_checks(
                Shop,
                members={"orders": ORDERS, "customers": CUSTOMERS, "refunds": ORDERS},
            )

        assert str(raised.value) == (
            "`members` names 'refunds', which is not a member of Shop. "
            "The members of Shop are 'orders', 'customers'."
        )

    def test_a_partitioned_member_raises(self):
        daily = dg.AssetSpec(
            "orders", partitions_def=dg.DailyPartitionsDefinition("2026-01-01")
        )

        with pytest.raises(InvalidMembersError) as raised:
            dd.collection_checks(
                Shop, members={"orders": daily, "customers": CUSTOMERS}
            )

        assert str(raised.value) == (
            "`members` maps 'orders' to a partitioned asset, "
            "and the checks read every member's whole table. Pass unpartitioned assets. "
            "The members of Shop are 'orders', 'customers'."
        )

    def test_a_definition_of_several_assets_raises(self):
        @dg.multi_asset(specs=[dg.AssetSpec("orders"), dg.AssetSpec("customers")])
        def warehouse(): ...

        with pytest.raises(InvalidMembersError) as raised:
            dd.collection_checks(
                Shop, members={"orders": warehouse, "customers": CUSTOMERS}
            )

        assert "maps 'orders' to a definition of several assets" in str(raised.value)

    def test_a_propagating_member_raises(self):
        class Propagating(dy.Collection):
            invoices: dy.DataFrame[Invoices]
            lines: Annotated[
                dy.DataFrame[InvoiceLines],
                dy.CollectionMember(propagate_row_failures=True),
            ]

        with pytest.raises(FailurePropagationError) as raised:
            dd.collection_checks(
                Propagating,
                members=INVOICE_MEMBERS,
            )

        assert (
            "Member 'lines' of Propagating sets `propagate_row_failures=True`"
            in str(raised.value)
        )

    def test_an_ignored_propagating_member_does_not_raise(self):
        class IgnoredPropagating(dy.Collection):
            invoices: dy.DataFrame[Invoices]
            lines: Annotated[
                dy.DataFrame[InvoiceLines],
                dy.CollectionMember(
                    ignored_in_filters=True, propagate_row_failures=True
                ),
            ]

        checks = dd.collection_checks(
            IgnoredPropagating,
            members=INVOICE_MEMBERS,
        )

        assert list(checks.check_specs) == []

    def test_a_propagating_optional_member_left_out_does_not_raise(self):
        class OptionalPropagating(dy.Collection):
            invoices: dy.DataFrame[Invoices]
            lines: Annotated[
                dy.DataFrame[InvoiceLines] | None,
                dy.CollectionMember(propagate_row_failures=True),
            ]

        checks = dd.collection_checks(
            OptionalPropagating, members={"invoices": dg.AssetSpec("invoices")}
        )

        assert list(checks.check_specs) == []

    def test_a_severity_for_something_other_than_a_collection_filter_raises(self):
        class Misdeclared(dy.Collection):
            __dagster_dataframely__ = {"severity": {"invoice_has_line": "warn"}}  # noqa: RUF012

            invoices: dy.DataFrame[Invoices]
            lines: dy.DataFrame[InvoiceLines]

            @dy.filter()
            def invoice_has_lines(self) -> pl.LazyFrame:
                return self.invoices.join(
                    self.lines, on="invoice_id", how="semi"
                ).lazy()

        with pytest.raises(InvalidSeverityError) as raised:
            dd.collection_checks(
                Misdeclared,
                members=INVOICE_MEMBERS,
            )

        assert str(raised.value) == (
            "`__dagster_dataframely__` on Misdeclared names 'invoice_has_line', "
            "which is not a collection filter of Misdeclared. "
            "The collection filters of Misdeclared are 'invoice_has_lines'."
        )


class TestRun:
    """A run of the checks over the member assets' tables."""

    def test_an_order_whose_customer_does_not_exist_fails_the_check(
        self, tmp_path: Path
    ):
        assets = shop_assets({"order_id": [10, 11, 12], "customer_id": [1, 2, 99]})

        result = dg.materialize(
            assets, resources=storage(tmp_path), raise_on_error=False
        )

        evaluation = evaluations(result)[ORDERS_CHECK]
        assert not evaluation.passed
        assert evaluation.severity == dg.AssetCheckSeverity.ERROR
        assert (
            evaluation.metadata["dataframely/filter"].value
            == "order_references_customer"
        )
        assert evaluation.metadata["dataframely/failed_row_count"].value == 1
        assert evaluation.description == "1 row failed."
        assert records(evaluation.metadata["dataframely/failed_sample"]) == [
            {"order_id": 12, "customer_id": 99}
        ]

    def test_clean_members_pass(self, tmp_path: Path):
        assets = shop_assets({"order_id": [10, 11], "customer_id": [1, 2]})

        result = dg.materialize(assets, resources=storage(tmp_path))

        evaluation = evaluations(result)[ORDERS_CHECK]
        assert evaluation.passed
        assert (
            evaluation.metadata["dataframely/filter"].value
            == "order_references_customer"
        )
        assert "dataframely/failed_row_count" not in evaluation.metadata
        assert "dataframely/failed_sample" not in evaluation.metadata
        assert evaluation.description == "No row failed."

    def test_a_failing_error_check_stops_its_members_downstream_asset(
        self, tmp_path: Path
    ):
        assets = shop_assets({"order_id": [10, 12], "customer_id": [1, 99]})

        @dg.asset
        def order_count(orders: pl.DataFrame) -> int:
            return orders.height

        result = dg.materialize(
            [*assets, order_count], resources=storage(tmp_path), raise_on_error=False
        )

        assert not result.success
        assert dg.AssetKey("order_count") not in {
            event.asset_key for event in result.get_asset_materialization_events()
        }

    def test_one_member_runs_alone_and_evaluates_only_its_checks(self, tmp_path: Path):
        @dd.asset(Invoices)
        def invoices() -> pl.DataFrame:
            return pl.DataFrame({"invoice_id": [1, 2]})

        @dd.asset(InvoiceLines)
        def invoice_lines() -> pl.DataFrame:
            return pl.DataFrame({"invoice_id": [1], "line": [1]})

        checks = dd.collection_checks(
            InvoiceBook, members={"invoices": invoices, "lines": invoice_lines}
        )
        assets = [invoices, invoice_lines, checks]
        instance = dg.DagsterInstance.ephemeral()
        dg.materialize(assets, resources=storage(tmp_path), instance=instance)

        result = dg.materialize(
            assets, resources=storage(tmp_path), instance=instance, selection="invoices"
        )

        assert result.success
        [evaluation] = evaluations(result).values()
        assert evaluation.asset_key == dg.AssetKey("invoices")
        assert not evaluation.passed
        assert evaluation.severity == dg.AssetCheckSeverity.WARN

    def test_an_eager_member_loads_through_a_pickling_io_manager(self, tmp_path: Path):
        @dg.asset
        def invoices() -> pl.DataFrame:
            return pl.DataFrame({"invoice_id": [1]})

        @dg.asset
        def invoice_lines() -> pl.DataFrame:
            return pl.DataFrame({"invoice_id": [1], "line": [1]})

        checks = dd.collection_checks(
            InvoiceBook, members={"invoices": invoices, "lines": invoice_lines}
        )

        result = dg.materialize(
            [invoices, invoice_lines, checks],
            resources={"io_manager": dg.FilesystemIOManager(base_dir=str(tmp_path))},
        )

        assert all(evaluation.passed for evaluation in evaluations(result).values())

    def test_an_optional_member_left_out_is_not_read(self, tmp_path: Path):
        assets = shop_assets({"order_id": [10], "customer_id": [1]})
        customers, orders, _ = assets
        checks = dd.collection_checks(
            ShopWithPromos, members={"orders": orders, "customers": customers}
        )

        result = dg.materialize(
            [customers, orders, checks], resources=storage(tmp_path)
        )

        assert {input_def.name for input_def in checks.node_def.input_defs} == {
            "orders",
            "customers",
        }
        assert all(evaluation.passed for evaluation in evaluations(result).values())
