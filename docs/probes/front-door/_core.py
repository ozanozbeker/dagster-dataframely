"""PROTOTYPE - throwaway (wayfinder #11). The kit every front-door shape builds on.

These are the parts the package must own no matter which shape wins:
spec derivation (#8), the reserved namespace + collision detection (#9),
definition metadata (#10/#14), and the runtime state machine (#6/#7).
The shapes differ only in who wires these together - the package or the user.
"""

import inspect
import os
from collections.abc import Iterator
from typing import Any, Literal

import dagster as dg
import dataframely as dy
import polars as pl

# Pin-and-assert obligation (#10): @public but not exported from dagster top-level.
from dagster._core.definitions.metadata.metadata_value import ObjectMetadataValue

RESERVED_PREFIX = "dy_"
GATE_CHECK = "dy_schema__dtypes"
SCHEMA_BUCKET_CHECK = "dy_schema__rules"
SCHEMA_CARRIER_KEY = "dagster_dataframely/schema"

Granularity = Literal["rule", "column", "schema"]
MultiColumnRules = Literal["schema", "per_rule"]

# Three-tier configuration (#8): package default < env var < per-asset argument.
# This ticket owns the env-var convention: DAGSTER_DATAFRAMELY_<SETTING>,
# fully qualified per #10's display-key vs machine-key rule.
_PACKAGE_DEFAULTS: dict[str, Any] = {
    "check_granularity": "rule",
    "multi_column_rules": "schema",
    "max_failure_samples": 0,  # value-bearing -> opt-in, off by default (#14)
}


def resolve_setting(name: str, per_asset: str | int | None) -> str | int:
    if per_asset is not None:
        return per_asset
    env = os.environ.get(f"DAGSTER_DATAFRAMELY_{name.upper()}")
    if env is not None:
        return env
    return _PACKAGE_DEFAULTS[name]


_GRANULARITY_VALUES: dict[str, Granularity] = {
    "rule": "rule",
    "column": "column",
    "schema": "schema",
}
_MULTI_COLUMN_RULES_VALUES: dict[str, MultiColumnRules] = {
    "schema": "schema",
    "per_rule": "per_rule",
}


def _resolve_choice[T: str](name: str, per_asset: T | None, allowed: dict[str, T]) -> T:
    """Resolve + validate: a garbage env var must be a loud package error (#8)."""
    value = resolve_setting(name, per_asset)
    resolved = allowed.get(str(value))
    if resolved is None:
        raise InvalidSettingError(name, value, tuple(allowed))
    return resolved


def resolve_granularity(per_asset: Granularity | None) -> Granularity:
    return _resolve_choice("check_granularity", per_asset, _GRANULARITY_VALUES)


def resolve_multi_column_rules(per_asset: MultiColumnRules | None) -> MultiColumnRules:
    return _resolve_choice("multi_column_rules", per_asset, _MULTI_COLUMN_RULES_VALUES)


class DagsterDataframelyError(Exception):
    """Base for the package's own loud errors - each subclass owns its message."""


class InvalidSettingError(DagsterDataframelyError):
    """A setting resolved (per-asset, env var, or default) to an unknown value (#8)."""

    def __init__(self, setting: str, value: object, allowed: tuple[str, ...]) -> None:
        super().__init__(
            f"Setting '{setting}' resolved to {value!r}; expected one of {allowed} "
            f"(tiers: per-asset argument > DAGSTER_DATAFRAMELY_{setting.upper()} > default)."
        )


class ReservedColumnError(DagsterDataframelyError):
    """A user column sits inside the reserved `dy_` namespace (#9)."""

    def __init__(self, schema_name: str, columns: list[str]) -> None:
        super().__init__(
            f"{schema_name}: column(s) {columns} use the reserved '{RESERVED_PREFIX}' "
            f"prefix; rename them - the package generates all names under this namespace."
        )


class CheckNameCollisionError(DagsterDataframelyError):
    """Two rules rewrite to the same asset-check name (#8/#9)."""

    def __init__(self, schema_name: str, first: str, second: str, name: str) -> None:
        super().__init__(
            f"{schema_name}: rules '{first}' and '{second}' both become asset-check "
            f"name '{name}' after the '|' -> '__' rewrite; rename one of them."
        )


class SchemaGateError(DagsterDataframelyError):
    """A pipeline defect (#7): wrong dtypes / missing columns. Whole-asset abort."""

    def __init__(self, schema_name: str, columns: list[str]) -> None:
        super().__init__(
            f"{len(columns)} column(s) do not match {schema_name}: {', '.join(columns)}"
        )


class ValidationAbortError(DagsterDataframelyError):
    """Rows rejected with no quarantine declared (#6). Both halves discarded."""

    def __init__(self, schema_name: str, rejected: int, counts: dict[str, int]) -> None:
        super().__init__(
            f"{rejected} row(s) rejected by {schema_name} and no quarantine is "
            f"declared; both halves discarded: {counts}"
        )


class NothingSurvivedError(DagsterDataframelyError):
    """Rows rejected and none survived (#6). Quarantine materialized, good out skipped."""

    def __init__(self, schema_name: str, rejected: int, counts: dict[str, int]) -> None:
        super().__init__(
            f"All {rejected} row(s) rejected by {schema_name}; good out skipped, "
            f"last-known-good preserved: {counts}"
        )


def check_name(rule_name: str) -> str:
    """`amount|min` -> `dy_rule__amount__min` - same string on every surface (#9)."""
    return f"dy_rule__{rule_name.replace('|', '__')}"


def _rules(schema: type[dy.Schema]) -> dict[str, Any]:
    # Pin-and-assert obligation (#8): private API, no public equivalent.
    return schema._validation_rules(with_cast=False)


def validate_namespace(schema: type[dy.Schema]) -> None:
    """The two loud errors Dagster would otherwise report opaquely (#8/#9)."""
    reserved = [c for c in schema.columns() if c.startswith(RESERVED_PREFIX)]
    if reserved:
        raise ReservedColumnError(schema.__name__, reserved)
    seen: dict[str, str] = {}
    for rule in _rules(schema):
        name = check_name(rule)
        if name in seen:
            raise CheckNameCollisionError(schema.__name__, seen[name], rule, name)
        seen[name] = rule


def _column_of(rule_name: str) -> str | None:
    col, sep, _ = rule_name.partition("|")
    return col if sep else None


def check_specs(
    schema: type[dy.Schema],
    *,
    # NOT dg.CoercibleToAssetKey: that name is typing-only - absent at runtime -
    # so evaluated annotations (no PEP 563 future import) cannot reference it.
    asset: str | dg.AssetKey,
    granularity: Granularity = "rule",
    multi_column_rules: MultiColumnRules = "schema",
) -> list[dg.AssetCheckSpec]:
    """Static spec derivation (#8): from the schema, never from a FailureInfo."""
    validate_namespace(schema)
    rules = _rules(schema)
    specs = [
        dg.AssetCheckSpec(
            GATE_CHECK,
            asset=asset,
            description=f"Columns and dtypes match {schema.__name__}.",
            blocking=True,
        )
    ]
    if granularity == "rule":
        for rule, obj in rules.items():
            doc = getattr(getattr(obj, "validation_fn", None), "__doc__", None)
            specs.append(
                dg.AssetCheckSpec(
                    check_name(rule),
                    asset=asset,
                    description=(doc or rule).strip(),
                )
            )
    elif granularity == "column":
        columns = sorted({c for r in rules if (c := _column_of(r))})
        specs += [
            dg.AssetCheckSpec(
                f"dy_col__{col}",
                asset=asset,
                description=f"All rules on column '{col}'.",
            )
            for col in columns
        ]
        multis = [r for r in rules if _column_of(r) is None]
        if multis and multi_column_rules == "schema":
            specs.append(
                dg.AssetCheckSpec(
                    SCHEMA_BUCKET_CHECK,
                    asset=asset,
                    description=f"Schema-level rules: {', '.join(sorted(multis))}.",
                )
            )
        elif multis:
            specs += [
                dg.AssetCheckSpec(check_name(r), asset=asset, description=r)
                for r in multis
            ]
    else:  # "schema"
        specs.append(
            dg.AssetCheckSpec(
                SCHEMA_BUCKET_CHECK,
                asset=asset,
                description=f"All {len(rules)} validation rules of {schema.__name__}.",
            )
        )
    return specs


def _constraint_pills(col: dy.Column) -> list[str]:
    """Minimal stand-in for #10's renderer ladder - enough to look real in print."""
    pills = []
    for attr, fmt in (("min", ">= {}"), ("max", "<= {}"), ("regex", "regex {}")):
        v = getattr(col, attr, None)
        if v is not None:
            pills.append(fmt.format(v))
    return pills


def table_schema(schema: type[dy.Schema]) -> dg.TableSchema:
    columns = [
        dg.TableColumn(
            name=name,
            type=str(col.dtype),
            description=col.description,
            constraints=dg.TableColumnConstraints(
                nullable=col.nullable, other=_constraint_pills(col)
            ),
        )
        for name, col in schema.columns().items()
    ]
    pk = [n for n, c in schema.columns().items() if c.primary_key]
    constraints = dg.TableConstraints(other=[f"PK: {', '.join(pk)}"]) if pk else None
    return dg.TableSchema(columns=columns, constraints=constraints)


def quarantine_table_schema(schema: type[dy.Schema]) -> dg.TableSchema:
    """Mirrored columns, no pills, one String outcome column per rule (#9/#10)."""
    columns = [
        dg.TableColumn(name=name, type=str(col.dtype))
        for name, col in schema.columns().items()
    ]
    columns += [
        dg.TableColumn(
            name=check_name(rule), type="String", description="valid/invalid/unknown"
        )
        for rule in _rules(schema)
    ]
    return dg.TableSchema(columns=columns)


def definition_metadata(schema: type[dy.Schema]) -> dict[str, Any]:
    """What the asset definition declares (#10/#14): the UI schema + the live carrier."""
    return {
        "dagster/column_schema": table_schema(schema),
        SCHEMA_CARRIER_KEY: ObjectMetadataValue(schema.__name__, instance=schema),
    }


_ASSET_OUT_PARAMS = [
    p for p in inspect.signature(dg.AssetOut.__init__).parameters if p != "self"
]


def rebuild_asset_out(out: dg.AssetOut, **force: Any) -> dg.AssetOut:
    """AssetOut is immutable with no _replace; rebuild it with overrides."""
    kwargs: dict[str, Any] = {}
    for p in _ASSET_OUT_PARAMS:
        if hasattr(out, p):
            kwargs[p] = getattr(out, p)
    kwargs.update(force)
    return dg.AssetOut(**kwargs)


def build_outs(
    asset_name: str,
    schema: type[dy.Schema],
    quarantine: dg.AssetOut | None,
    *,
    key: dg.AssetKey | None = None,
    key_prefix: list[str] | None = None,
) -> dict[str, dg.AssetOut]:
    """Both outs is_required=False (#6). Quarantine name defaults to a sibling (#9)."""
    good_kwargs: dict[str, Any] = {
        "is_required": False,
        "metadata": definition_metadata(schema),
    }
    if key is not None:
        good_kwargs["key"] = key
    outs = {asset_name: dg.AssetOut(**good_kwargs)}
    if quarantine is not None:
        q_meta = dict(getattr(quarantine, "metadata", None) or {})
        q_meta.setdefault("dagster/column_schema", quarantine_table_schema(schema))
        force: dict[str, Any] = {"is_required": False, "metadata": q_meta}
        # The default sibling inherits the good asset's prefix (#9); an explicit
        # AssetOut(key=) from the user overrides completely.
        if key_prefix and getattr(quarantine, "key", None) is None:
            force["key_prefix"] = key_prefix
        outs[f"{asset_name}_quarantine"] = rebuild_asset_out(quarantine, **force)
    return outs


def quarantine_frame(schema: type[dy.Schema], failure: dy.FailureInfo) -> pl.DataFrame:
    """details() with outcome columns renamed into the namespace and cast off Enum (#9)."""
    details = failure.details()
    original = set(schema.columns())
    renames = {c: check_name(c) for c in details.columns if c not in original}
    return details.rename(renames).with_columns(
        pl.col(name).cast(pl.String) for name in renames.values()
    )


def _check_results(
    schema: type[dy.Schema],
    counts: dict[str, int],
    *,
    granularity: Granularity,
    multi_column_rules: MultiColumnRules,
    severity: dg.AssetCheckSeverity,
) -> list[dg.AssetCheckResult]:
    rules = _rules(schema)

    def result(name: str, members: list[str]) -> dg.AssetCheckResult:
        failed = {r: counts[r] for r in members if counts.get(r)}
        metadata: dict[str, Any] = {
            "dy_rule": ", ".join(members),
            "dy_rule__expr": "; ".join(str(rules[r].expr) for r in members),
        }
        if failed:
            metadata["dy_failed_count"] = sum(failed.values())
            # Opt-in bounded per-rule samples (dy_rule__samples) would land here -
            # value-bearing, so off by default (#8/#14).
        return dg.AssetCheckResult(
            check_name=name, passed=not failed, severity=severity, metadata=metadata
        )

    if granularity == "rule":
        return [result(check_name(r), [r]) for r in rules]
    if granularity == "column":
        columns = sorted({c for r in rules if (c := _column_of(r))})
        out = [
            result(f"dy_col__{col}", [r for r in rules if _column_of(r) == col])
            for col in columns
        ]
        multis = [r for r in rules if _column_of(r) is None]
        if multis and multi_column_rules == "schema":
            out.append(result(SCHEMA_BUCKET_CHECK, multis))
        elif multis:
            out += [result(check_name(r), [r]) for r in multis]
        return out
    return [result(SCHEMA_BUCKET_CHECK, list(rules))]


def process(
    schema: type[dy.Schema],
    frame: pl.DataFrame | pl.LazyFrame,
    *,
    context: dg.AssetExecutionContext,
    good_out: str,
    quarantine_out: str | None = None,
    granularity: Granularity = "rule",
    multi_column_rules: MultiColumnRules = "schema",
) -> Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]:
    """The runtime state machine settled by #6/#7 - shared by every shape."""
    good_key = context.asset_key_for_output(good_out)

    # 1. The schema gate (#7): explicit pre-check, public API only, never try/except.
    actual = frame.collect_schema()
    problems = [
        {
            "column": name,
            "expected": str(col.dtype),
            "actual": str(actual[name]) if name in actual else "<missing>",
        }
        for name, col in schema.columns().items()
        if name not in actual or not col.validate_dtype(actual[name])
    ]
    if problems:
        yield dg.AssetCheckResult(
            check_name=GATE_CHECK,
            asset_key=good_key,
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={
                "dy_schema__errors": dg.MetadataValue.table(
                    [dg.TableRecord(p) for p in problems]
                )
            },
        )
        raise SchemaGateError(schema.__name__, [p["column"] for p in problems])
    gate_pass = dg.AssetCheckResult(
        check_name=GATE_CHECK, asset_key=good_key, passed=True
    )

    # 2. Filter (#6): always filter, never validate. Good half is handed on eager -
    #    filter already collected internally, and row_count needs the length anyway.
    result, failure = schema.filter(frame, cast=False)
    good = result.collect() if isinstance(result, pl.LazyFrame) else result
    n_good, n_rejected = len(good), len(failure)

    aborting = n_rejected > 0 and (quarantine_out is None or n_good == 0)
    severity = dg.AssetCheckSeverity.ERROR if aborting else dg.AssetCheckSeverity.WARN
    checks = [
        gate_pass,
        *_check_results(
            schema,
            failure.counts(),
            granularity=granularity,
            multi_column_rules=multi_column_rules,
            severity=severity,
        ),
    ]
    good_metadata: dict[str, Any] = {"dagster/row_count": n_good}

    # 3. The four outcomes (#6): the asset's declared shape is the policy.
    if n_rejected == 0:
        yield dg.MaterializeResult(
            asset_key=good_key, value=good, metadata=good_metadata, check_results=checks
        )
        return  # quarantine out skipped: nothing was rejected

    if quarantine_out is None:
        for check in checks:  # per-rule history still lands on the failed run (#8)
            yield check
        raise ValidationAbortError(schema.__name__, n_rejected, failure.counts())

    cooccurrence = [
        dg.TableRecord({"rules": ", ".join(sorted(k)), "count": v})
        for k, v in failure.cooccurrence_counts().items()
    ]
    quarantine_result = dg.MaterializeResult(
        asset_key=context.asset_key_for_output(quarantine_out),
        value=quarantine_frame(schema, failure),
        metadata={
            "dagster/row_count": n_rejected,
            "dy_cooccurrence": dg.MetadataValue.table(cooccurrence),
        },
    )

    if n_good == 0:
        yield quarantine_result
        for check in checks:
            yield check
        raise NothingSurvivedError(schema.__name__, n_rejected, failure.counts())

    yield dg.MaterializeResult(
        asset_key=good_key, value=good, metadata=good_metadata, check_results=checks
    )
    yield quarantine_result
