"""`dd.build_collection_checks` reports a Dataframely Collection's collection filters as asset checks on the member assets the user declares.

The checks read the tables those assets wrote, and they evaluate only the collection filters.
They never change a table: `docs/out-of-scope/collection-assets.md` records why this package builds no asset from a Collection.
"""

import inspect
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass

import dagster as dg
import dataframely as dy
import polars as pl

from dagster_dataframely._checks import describe_failed_rows
from dagster_dataframely._naming import get_check_name_for_filter
from dagster_dataframely._rules import Severity, get_filter_severities
from dagster_dataframely._samples import build_sample_metadata, sample_rows
from dagster_dataframely._settings import MAX_FAILURE_EXAMPLES
from dagster_dataframely.errors import (
    FailurePropagationError,
    InvalidMembersError,
    _quote,
)

type Members = Mapping[str, dg.AssetsDefinition | dg.AssetSpec]


@dataclass(frozen=True)
class _FilterCheck:
    """The asset check for one collection filter on one member."""

    filter_name: str
    member: str
    spec: dg.AssetCheckSpec


def _to_snake_case(name: str) -> str:
    """Return a class name in snake case: `HTTPLogs` becomes `http_logs`."""
    spaced: str = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", spaced).lower()


def _validate_members(collection: type[dy.Collection], members: Members) -> None:
    """Raise unless `members` maps every required member, and only members, to one unpartitioned asset each."""
    names: list[str] = list(collection.members())
    owner: str = collection.__name__
    unknown: list[str] = [member for member in members if member not in names]
    if unknown:
        problem = (
            f"names {_quote(unknown)}, which is not a member of {owner}."
            if len(unknown) == 1
            else f"names {_quote(unknown)}, which are not members of {owner}."
        )
        raise InvalidMembersError(owner, problem, names)
    required: set[str] = collection.required_members()
    missing: list[str] = [
        member for member in names if member in required and member not in members
    ]
    if missing:
        plural: str = "" if len(missing) == 1 else "s"
        raise InvalidMembersError(
            owner,
            f"lacks the required member{plural} {_quote(missing)} of {owner}.",
            names,
        )
    several: list[str] = [
        member
        for member, asset in members.items()
        if isinstance(asset, dg.AssetsDefinition) and len(asset.keys) != 1
    ]
    if several:
        raise InvalidMembersError(
            owner,
            f"maps {_quote(several)} to a definition of several assets. Pass the member's `dg.AssetSpec` from its `specs`.",
            names,
        )
    partitioned: list[str] = [
        member for member, asset in members.items() if asset.partitions_def is not None
    ]
    if partitioned:
        assets: str = (
            "a partitioned asset" if len(partitioned) == 1 else "partitioned assets"
        )
        raise InvalidMembersError(
            owner,
            f"maps {_quote(partitioned)} to {assets}, and the checks read every member's whole table. Pass unpartitioned assets.",
            names,
        )


def _build_filter_checks(
    collection: type[dy.Collection],
    keys: Mapping[str, dg.AssetKey],
    severities: Mapping[str, Severity],
) -> list[_FilterCheck]:
    """Return the check for each collection filter on each mapped member without `ignored_in_filters=True`."""
    infos = collection.members()
    return [
        _FilterCheck(
            filter_name,
            member,
            dg.AssetCheckSpec(
                get_check_name_for_filter(filter_name),
                asset=key,
                description=inspect.getdoc(collection_filter.logic) or filter_name,
                additional_deps=[
                    other for name, other in keys.items() if name != member
                ],
                blocking=severities[filter_name] == "error",
            ),
        )
        for filter_name, collection_filter in collection._filters().items()  # noqa: SLF001
        for member, key in keys.items()
        if not infos[member].ignored_in_filters
    ]


def build_collection_checks(
    collection: type[dy.Collection],
    /,
    *,
    members: Members,
    name: str | None = None,
    max_failure_examples: int | None = None,
) -> dg.AssetChecksDefinition:
    """Return an asset check for each collection filter of `collection` on each of its member assets.

    Parameters
    ----------
    collection
        The Collection whose `@dy.filter` methods become checks.
    members
        The asset for each member of `collection`, keyed by member name: the `dg.AssetsDefinition` that `dd.asset` returns, or a `dg.AssetSpec`.
    name
        The name of the step that runs the checks. `None` uses the Collection's class name in snake case.
    max_failure_examples
        A check's metadata has at most this many rows that failed its collection filter. `None` uses `DAGSTER_DATAFRAMELY_MAX_FAILURE_EXAMPLES` if set, else `5`.

    Returns
    -------
    :
        One definition with every check, which Dagster runs in one step. Pass it to `dg.Definitions(asset_checks=[...])`.

    Raises
    ------
    InvalidMembersError
        `members` leaves out a required member of `collection`, names something that is not a member, or maps a member to a partitioned asset or a definition of several assets.
    FailurePropagationError
        A member in `members` sets `propagate_row_failures=True` without `ignored_in_filters=True`.
    InvalidSeverityError
        The Collection declares a severity this package cannot read.
    InvalidSettingError
        A setting's argument or environment variable has a value the setting does not allow.

    Examples
    --------
    `order_has_customer` keeps each order whose `email` is a customer's. `customers` sets `ignored_in_filters=True`, so the check reports on `orders` alone:

    ```python
    from typing import Annotated

    import dataframely as dy
    import polars as pl

    import dagster_dataframely as dd
    from dagster_dataframely.data import OrdersSchema, storefront_orders


    class CustomersSchema(dy.Schema):
        email = dy.String(primary_key=True)


    class Shop(dy.Collection):
        orders: dy.LazyFrame[OrdersSchema]
        customers: Annotated[
            dy.LazyFrame[CustomersSchema], dy.CollectionMember(ignored_in_filters=True)
        ]

        @dy.filter()
        def order_has_customer(self) -> pl.LazyFrame:
            return self.orders.join(self.customers, on="email", how="semi")


    @dd.asset(OrdersSchema)
    def orders() -> pl.DataFrame:
        return storefront_orders


    @dd.asset(CustomersSchema)
    def customers() -> pl.DataFrame:
        return storefront_orders.select("email").unique()


    shop_checks = dd.build_collection_checks(
        Shop, members={"orders": orders, "customers": customers}
    )
    [(spec.asset_key.to_user_string(), spec.name) for spec in shop_checks.check_specs]
    ```
    """
    _validate_members(collection, members)
    infos = collection.members()
    # A member left out of `members` is not read, so only a mapped one can propagate.
    propagating: list[str] = [
        member
        for member in members
        if infos[member].propagate_row_failures and not infos[member].ignored_in_filters
    ]
    if propagating:
        raise FailurePropagationError(collection.__name__, propagating)
    keys: dict[str, dg.AssetKey] = {
        member: asset.key for member, asset in members.items()
    }
    severities: dict[str, Severity] = get_filter_severities(collection)
    limit: int = MAX_FAILURE_EXAMPLES.resolve(max_failure_examples)
    checks: list[_FilterCheck] = _build_filter_checks(collection, keys, severities)
    # Dagster rejects an input that no spec depends on, so a Collection with nothing to check reads nothing.
    inputs: dict[str, dg.AssetKey] = keys if checks else {}

    def _results(
        context: dg.AssetCheckExecutionContext, **frames: pl.DataFrame | pl.LazyFrame
    ) -> Iterator[dg.AssetCheckResult]:
        lazy_frames: dict[str, pl.LazyFrame] = {
            member: frame.lazy() for member, frame in frames.items()
        }
        _, failure = collection.filter(lazy_frames, skip_member_validation=True)
        for check in checks:
            if check.spec.key not in context.selected_asset_check_keys:
                continue
            failed: int = failure[check.member].counts().get(check.filter_name, 0)
            metadata: dict[str, str | int | dg.TableMetadataValue] = {
                "dataframely/filter": check.filter_name
            }
            if failed:
                info: dy.FailureInfo = failure[check.member]
                metadata["dataframely/failed_row_count"] = failed
                metadata |= build_sample_metadata(
                    "dataframely/failed_examples",
                    sample_rows(
                        info.invalid().filter(
                            info.details()[check.filter_name] == "invalid"
                        ),
                        limit,
                    ),
                )
            yield dg.AssetCheckResult(
                asset_key=check.spec.asset_key,
                check_name=check.spec.name,
                passed=not failed,
                severity=dg.AssetCheckSeverity.ERROR
                if severities[check.filter_name] == "error"
                else dg.AssetCheckSeverity.WARN,
                description=describe_failed_rows(failed),
                metadata=metadata,
            )

    # Dagster reads each input's name and type from the signature, so build one from the members.
    _results.__signature__ = inspect.Signature([  # pyrefly: ignore[missing-attribute]
        inspect.Parameter(
            "context",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            annotation=dg.AssetCheckExecutionContext,
        ),
        *(
            inspect.Parameter(
                member,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=pl.LazyFrame if infos[member].is_lazy else pl.DataFrame,
            )
            for member in inputs
        ),
    ])
    return dg.multi_asset_check(
        specs=[check.spec for check in checks],
        ins={member: dg.AssetIn(key=key) for member, key in inputs.items()},
        name=name or _to_snake_case(collection.__name__),
        can_subset=True,
    )(_results)
