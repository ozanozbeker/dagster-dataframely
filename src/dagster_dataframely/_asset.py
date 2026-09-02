"""The decorator: one argument attaches a Dataframely schema to a Dagster asset.

The decorator coordinates three artifacts no single `@dg.asset` parameter accepts as a bundle: the check specs, the definition metadata, and the wrapped runtime. First-party precedent for a decorator that does this is `@dbt_assets`.

**One `dg.asset` call, not a `multi_asset`.** The quarantine stopped being a `dg.AssetOut` in ADR-0004, so there is nothing for a second output to hold and nothing for `internal_asset_deps` to wire. See ADR-0005 for why the decorator wraps `dg.asset` rather than stacking under it: a check spec is an op output, and no public API adds one to a finished `AssetsDefinition` or swaps its compute function.

This module carries no `from __future__ import annotations`. At a 3.12 floor it would buy only unquoted forward references, while turning user-facing annotations into strings that Dagster's runtime introspection rejects. The cost is that typing-only names such as `dg.CoercibleToAssetDep` are absent at runtime, so they are spelled here with runtime-real types.
"""

import functools
import inspect
from collections.abc import Callable, Iterable, Mapping, Sequence
from collections.abc import Set as AbstractSet
from typing import Any

import dagster as dg
import dataframely as dy

# Upstream's own rule for whether a decorated function asked for a context, imported
# rather than restated so the wrapper and Dagster cannot disagree about one function.
from dagster._core.definitions.decorators.op_decorator import is_context_provided

# Raised by every context property that only a real step can answer, which is how the
# wrapper tells a run from a direct invocation. Both are pinned by characterization tests.
from dagster._core.errors import DagsterInvalidPropertyError

from dagster_dataframely._checks import check_specs
from dagster_dataframely._metadata import schema_metadata
from dagster_dataframely._quarantine import (
    QuarantineWriter,
    delegating_writer,
    file_writer,
)
from dagster_dataframely._returns import DecoratedReturn, fold, unwrap
from dagster_dataframely._runtime import AssetYield, process
from dagster_dataframely._settings import (
    CHECK_GRANULARITY,
    MAX_FAILURE_SAMPLES,
    MULTI_COLUMN_RULES,
    QUARANTINE_DIR,
    ROW_SAMPLE,
    STATISTICS,
    TEMP_DIR,
    Granularity,
    MultiColumnRules,
)
from dagster_dataframely.errors import (
    CollectionNotSupportedError,
    QuarantineDirError,
)

DecoratedFn = Callable[..., DecoratedReturn]

# The union `@dg.asset` accepts, spelled out because `AutomationCondition` is generic and its two parameterizations are not interchangeable.
AutomationCondition = (
    dg.AutomationCondition[dg.AssetKey]
    | dg.AutomationCondition[dg.AssetKey | dg.AssetCheckKey]
)

#: Runtime-real spelling of Dagster's `CoercibleToAssetDep`, which is typing-only.
AssetDep = (
    dg.AssetKey
    | str
    | Sequence[str]
    | dg.AssetSpec
    | dg.AssetsDefinition
    | dg.SourceAsset
    | dg.AssetDep
)


def _quarantine_writer(
    context: dg.AssetExecutionContext, *, quarantine_dir: str | None
) -> QuarantineWriter:
    """Choose who writes this run's invalid rows.

    Delegation first, always. The asset's own IO manager puts the rows wherever it puts things, which needs no configuration and cannot disagree with where the valid table went (ADR-0006).

    `file_writer` answers the one case with no step: direct invocation, where the asset is called rather than run. A test that wants the real placement runs the asset.

    **The step is asked for on its own, ahead of the writer.** There is no predicate that answers "is this a run", so the question has to be put as a call that raises. Wrapping the whole of `delegating_writer` in that guard would widen it: any other property it reads raising the same error inside a real run would silently reroute the rows to a file.

    Parameters
    ----------
    context
        The executing asset's context.
    quarantine_dir
        Where the fallback writes, from `DAGSTER_DATAFRAMELY_QUARANTINE_DIR`, or `None` when the deployment named none.

    Returns
    -------
    The writer to hand `process`.

    Raises
    ------
    QuarantineDirError
        There is no manager to delegate to and no quarantine_dir to fall back on.
    """
    try:
        context.get_step_execution_context()
    except DagsterInvalidPropertyError:
        # Not a run, so there is no step, no output context and no manager behind it.
        pass
    else:
        return delegating_writer(context)
    if quarantine_dir is None:
        raise QuarantineDirError(context.asset_key.to_user_string())
    return file_writer(
        context.asset_key,
        quarantine_dir,
        # Read behind the guard because `partition_key` raises on an unpartitioned asset rather than answering `None`.
        context.partition_key if context.has_partition_key else None,
    )


def _description(schema: type[dy.Schema], description: str | None) -> str | None:
    """Resolve the asset's description, most specific source first.

    What the decorator was passed wins, then the schema's own docstring. Returning `None` leaves Dagster's fallback to the decorated function's docstring standing, so the package fills the gap rather than closing it.

    The schema outranks it because the schema is what describes the table, whereas the function's docstring describes the code that fills it. This is the opposite precedence from `metadata`, where the package's own key is applied over the user's. That key is this package's surface and a collision is a mistake, while a description is prose the author owns.

    Empty is absent on both sources, and neither can express "no description at all", because Dagster's own fallback takes over as soon as this returns nothing.

    Parameters
    ----------
    schema
        The schema the asset validates against.
    description
        What the decorator was given, or `None`.

    Returns
    -------
    The description to forward, or `None` to leave Dagster's own fallback in place.
    """
    # Read through `__doc__` rather than `inspect.getdoc`, which walks the MRO: a schema declaring no docstring would inherit `dy.Schema`'s and describe itself as a base class for schema definitions.
    # `cleandoc` because a raw docstring keeps its source indentation, which the catalog renders as a code block.
    return description or inspect.cleandoc(schema.__doc__ or "") or None


def dy_asset(  # noqa: PLR0913 - forwarding the whole parameter list is the point
    schema: type[dy.Schema],
    /,
    *,
    # --- decorator-owned ---
    quarantine: bool = False,
    check_granularity: Granularity | None = None,
    multi_column_rules: MultiColumnRules | None = None,
    max_failure_samples: int | None = None,
    statistics: bool | None = None,
    row_sample: int | None = None,
    temp_dir: str | None = None,
    # --- forwarded to @dg.asset, verbatim, except `description`, which resolves against the schema first, and `metadata`, which the schema's Columns tab is applied over ---
    name: str | None = None,
    key_prefix: str | Sequence[str] | None = None,
    ins: Mapping[str, dg.AssetIn] | None = None,
    deps: Iterable[AssetDep] | None = None,
    metadata: Mapping[str, Any] | None = None,
    tags: Mapping[str, str] | None = None,
    description: str | None = None,
    # --- Narrower than Dagster's own six-member union, deliberately: a mapping is the spelling worth a static guarantee, and the rest are legacy. ---
    config_schema: Mapping[str, Any] | None = None,
    required_resource_keys: AbstractSet[str] | None = None,
    resource_defs: Mapping[str, object] | None = None,
    hooks: AbstractSet[dg.HookDefinition] | None = None,
    io_manager_key: str | None = None,
    partitions_def: dg.PartitionsDefinition[str] | None = None,
    op_tags: Mapping[str, Any] | None = None,
    group_name: str | None = None,
    automation_condition: AutomationCondition | None = None,
    freshness_policy: dg.FreshnessPolicy | None = None,
    backfill_policy: dg.BackfillPolicy | None = None,
    retry_policy: dg.RetryPolicy | None = None,
    code_version: str | None = None,
    owners: Sequence[str] | None = None,
    kinds: AbstractSet[str] | None = None,
    pool: str | None = None,
) -> Callable[[DecoratedFn], dg.AssetsDefinition]:
    """Turn the decorated function into an asset that validates the frame it returns against `schema`.

    The contract then lives in exactly one place. From the single declaration, the Columns tab fills in before the asset has ever run, every Dataframely rule reports through an asset check with pass/fail history, one check per rule until `check_granularity` collapses them, and a frame whose shape does not match the schema aborts the run before a single row is filtered.

    The decorated function keeps plain Polars annotations. Nothing rewrites the signature, upstream dependencies bind as ordinary parameters, and a declared `context` binds the way it does on a plain `@dg.asset`. Five returns are accepted: a frame or a `dg.MaterializeResult` carrying one, eager or lazy, or `None`:

        pl.DataFrame                dg.MaterializeResult[pl.DataFrame]
        pl.LazyFrame                dg.MaterializeResult[pl.LazyFrame]
        None

    **The object returned decides what happens, never the annotation.** A `LazyFrame` streams to a local parquet before it is validated whichever way the signature spells it. `@dg.asset` does hold you to its annotation, by inferring the output's `dagster_type` from it and failing the run on a mismatch. This decorator cannot: the annotation describes what the decorated function handed over, while `dagster_type` describes what the asset stores, and those differ here. Validation is eager, so the asset always holds a `DataFrame` however the decorated function arrived at it. Annotate it anyway and a type checker holds you to it instead. Parameterize a returned result when you do, since a bare `dg.MaterializeResult` is an implicit `Any` that a strict checker rejects.

    **Returning `None` skips the asset.** Nothing is validated, nothing materializes, and the run stays green, so a partition with no source data stays unmaterialized instead of going green with zero rows or red with an error. It is for the partition that has no data and never will, which is neither an empty report nor a broken pipeline:

        def sales(context: dg.AssetExecutionContext) -> pl.DataFrame | None:
            path = source_path(context.partition_key)
            if not path.exists():
                return None
            return pl.read_parquet(path)

    The test is yours to write. The decorator never catches `FileNotFoundError`, or anything else, to decide this for you: it cannot tell a file that is legitimately absent from a path that is misconfigured, and guessing wrong turns a broken pipeline into a silently missing partition. `None` is the word for the first case, and an escaping error stays the word for the second.

    Every check still reports on a skipped run, and passes. A check spec is a non-optional output whatever the asset declares, so a step that answers none of them fails outright. The rules are therefore run over an empty frame and report what that says. Nothing is fabricated: each rule was evaluated, over zero rows, and none was violated. Dagster attaches those evaluations to no materialization, so a passing check on a skipped partition does not claim to have checked an earlier one.

    A returned result is what `@dg.asset` accepts and the only route there is to a materialization's tags and data version:

        def orders(raw_orders: pl.DataFrame) -> dg.MaterializeResult[pl.DataFrame]:
            return dg.MaterializeResult(value=raw_orders, metadata={"source": "stripe"})

    Its `value` is the frame to validate, and is required. `dg.MaterializeResult(value=None)` is refused rather than read as the skip, because the whole point of a returned result is to put something on a materialization and a skipped run has none. Its `metadata`, `data_version` and `tags` land on the materialization, the package's own metadata keys winning a collision exactly as they do for `metadata=` above. `asset_key` and `check_results` are refused by name, because the decorator decides both.

    A returned result is the route this package prefers, and the context is the other one. `context.add_asset_metadata({...})` reaches the same materialization. That route also overrides this package's own metadata keys, where a returned result loses to them, and it cannot be reached by calling the asset.

    **The asset's declaration is the failure policy.** There is no lenient mode and no strict flag, deliberately. `quarantine=True` *is* the consent to partial data, so what an invalid row costs is visible in the definition and cannot disagree with what the asset declares. The skip sits outside this. It says there were no rows to have a policy about.

    With no quarantine, every row has to be valid. A run with even one failing row writes nothing, leaving the last-known-good table in place. To drop rows anyway, filter in the asset body, where the drop is a line you wrote:

        valid, _ = Orders.filter(raw_orders)
        return valid

    With a quarantine, the invalid rows are handed to the asset's own IO manager under the key `<name>_quarantine`, carrying the original columns plus a rule column for every rule. They land wherever that manager puts things: a parquet file beside the table under `dagster-polars`, a second table beside it under `dagster-duckdb-polars`. The checks then fail at `WARN` and the run stays green, so downstream proceeds on the data that is fine. The materialization records where the rows went, how many there were, which sets of rules they failed together and a sample. A clean run writes no quarantine, and a run where *nothing* survived writes the quarantine and skips the asset rather than emptying it.

    Every parameter is declared explicitly with its runtime-real type, so editors autocomplete them and `group_nme="sales"` is a static error rather than an import-time crash. `check_specs` is a parameter this decorator owns and is simply absent, so it cannot be contested.

    Parameters
    ----------
    schema
        The Dataframely schema the decorated function's output must satisfy. Positional-only, and the only parameter that is: it is the whole reason this decorator exists, so it is required and it is never one keyword among thirty.
    quarantine
        Whether invalid rows are kept. `True` is the consent to partial data, `False` the refusal. It needs no configuration, because it means "wherever this asset's manager puts things". **`True` adds a `context` parameter** to the asset, whether or not the decorated function declared one, because the writer is built from the execution context and nothing else can reach it: calling a quarantined asset directly therefore takes a `dg.build_asset_context()` first. Calling one reaches no IO manager, so there the rows go to a parquet file under `DAGSTER_DATAFRAMELY_QUARANTINE_DIR`, and raise if that is unset.
    check_granularity
        How far the schema's rules collapse into checks. `rule` gives each rule its own check and its own history. `column` gives one check per rule-bearing column, `dy_col__<column>`, which is what makes a wide schema's check list readable. `schema` gives a single `dy_schema__rules` for all of them. **Changing this on an existing asset orphans check history**: the old check names stop being reported and their timelines end where the change landed, while the new ones start empty. Nothing migrates them, so choose it before the asset ships rather than after. Unset resolves through `DAGSTER_DATAFRAMELY_CHECK_GRANULARITY`, then the package default `rule`.
    multi_column_rules
        Where the rules no single column owns land at `column` granularity: grouped into `dy_schema__rules`, or `per_rule` for a check each. Read at no other granularity, because neither has a second place to put them. Unset resolves through `DAGSTER_DATAFRAMELY_MULTI_COLUMN_RULES`, then the package default `schema`.
    max_failure_samples
        How many of the rows that failed a rule reach that rule's check metadata, under `dy_failed_sample`. What a red check raises and the counts cannot answer, so it is opt-out and `0` is what turns it off. **These are real rows in the Dagster event log**, which is shared, exported and not redacted. The bound is this package's own, and `dy.Config.set_max_failure_examples` does not touch it. Bounded per rule, so a collapsed check shows this many for each rule anything failed. Unset resolves through `DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES`, then the package default `5`.
    statistics
        Whether the materialization carries `skimr`-style statistics for what it wrote: one table per dtype family present. Opt-out rather than opt-in, so `False` is what turns the pass off. The string family deliberately carries no value-bearing statistic at either value, only lengths and cardinality: consenting to summary statistics is not consenting to raw values. That is what the two sample settings are for, which is why they are separate from this one. The quarantine carries none at any value, because nothing consumes it. Unset resolves through `DAGSTER_DATAFRAMELY_STATISTICS`, then the package default `true`.
    row_sample
        How many rows reach the materialization metadata, of what was written under `dataframely/valid_sample` and of what was held back under `dataframely/invalid_sample`. Opt-out on the same terms as `max_failure_samples`, with the same consequence: **these are real rows in the event log**, and `0` is what turns them off. One number for both, so consenting to a sample is one decision. Unset resolves through `DAGSTER_DATAFRAMELY_ROW_SAMPLE`, then the package default `5`.
    temp_dir
        Where a `LazyFrame` return is staged before it is validated. Read on that path only, so an asset returning a `DataFrame` is unaffected by it. **Unset, the staging file goes to the system temp directory, which in a container is its ephemeral disk**, and a staged frame bigger than what the pod has spare fills it. Pointing this at a mounted volume is the fix. A directory that does not exist raises rather than being created, because a mistyped path silently created on that disk is the failure this setting was set to avoid. Unset resolves through `DAGSTER_DATAFRAMELY_TEMP_DIR`.
    name
        Asset name. Defaults to the function name.
    key_prefix
        Prefix for the asset key. The checks and the quarantine follow it automatically.
    ins
        Explicit input mapping, for the cases a parameter name cannot express.
    deps
        Upstream assets this one depends on without loading.
    metadata
        Definition metadata to carry alongside the schema's own. `dagster/column_schema` is the package's and wins a collision.
    tags
        Asset tags, for filtering and grouping in the catalog.
    description
        Asset description. Unset, the schema's own docstring fills it, and the decorated function's docstring stands only where the schema has none.
    config_schema
        Run configuration schema for the underlying op.
    required_resource_keys
        Resources the decorated function reaches through the context. The quarantine needs none: its manager comes off the step rather than read off the context's resources.
    resource_defs
        Resources bound to this asset specifically.
    hooks
        Hooks to attach to the underlying op.
    io_manager_key
        Resource key the table is stored under. The quarantine goes to the same manager, so moving one moves both.
    partitions_def
        Partitioning for the asset. Validation then runs per partition, on that partition's frame, and the quarantine is written under the same partition, so it cannot escape its asset's partitioning.
    op_tags
        Tags on the underlying op, for run launcher and executor routing.
    group_name
        Asset group.
    automation_condition
        Declarative automation condition for the asset.
    freshness_policy
        Freshness policy for the asset.
    backfill_policy
        How Dagster backfills this asset's partitions.
    retry_policy
        Retry policy for the underlying op.
    code_version
        Version string for change-based staleness.
    owners
        Asset owners, as emails or `team:<name>`.
    kinds
        Kind badges shown on the asset in the graph.
    pool
        Concurrency pool the underlying op runs in.

    Returns
    -------
    A decorator producing a `dg.asset` carrying the checks `check_granularity` asks for.

    Raises
    ------
    CollectionNotSupportedError
        `schema` is a `dy.Collection`.
    InvalidSettingError
        A setting resolved to a value outside its vocabulary, from any source.

    Examples
    --------
    ```python
    import dagster as dg
    import dataframely as dy
    import polars as pl
    import dagster_dataframely as dd


    class Orders(dy.Schema):
        order_id = dy.String(primary_key=True)
        amount = dy.Float64(nullable=False, min=0.0)


    @dd.dy_asset(Orders, quarantine=True, group_name="sales")
    def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
        return raw_orders.select("order_id", "amount")
    ```
    """
    # Deliberately narrow: anything else keeps failing however it already fails.
    if isinstance(schema, type) and issubclass(schema, dy.Collection):
        raise CollectionNotSupportedError(schema.__name__)

    # Resolved once, here, and handed to both the specs and the runtime. Resolving again inside the run would read the executing process's environment, so a worker with a different `DAGSTER_DATAFRAMELY_*` would report against checks the code location never declared. The last four affect nothing built at definition time, but they resolve here too: a mistyped environment variable then fails where the asset is declared rather than on whichever run happens to reach it first.
    granularity: Granularity = CHECK_GRANULARITY.resolve(check_granularity)
    multi_column: MultiColumnRules = MULTI_COLUMN_RULES.resolve(multi_column_rules)
    failure_samples: int = MAX_FAILURE_SAMPLES.resolve(max_failure_samples)
    emit_statistics: bool = STATISTICS.resolve(statistics)
    sampled_rows: int = ROW_SAMPLE.resolve(row_sample)
    staging_dir: str | None = TEMP_DIR.resolve(temp_dir)
    quarantine_root: str | None = QUARANTINE_DIR.resolve(None)

    forwarded: dict[str, Any] = {
        "ins": ins,
        "deps": deps,
        "tags": tags,
        "description": _description(schema, description),
        "config_schema": config_schema,
        "required_resource_keys": required_resource_keys,
        "resource_defs": resource_defs,
        "hooks": hooks,
        "io_manager_key": io_manager_key,
        "partitions_def": partitions_def,
        "op_tags": op_tags,
        "group_name": group_name,
        "automation_condition": automation_condition,
        "freshness_policy": freshness_policy,
        "backfill_policy": backfill_policy,
        "retry_policy": retry_policy,
        "code_version": code_version,
        "owners": list(owners) if owners else None,
        "kinds": set(kinds) if kinds else None,
        "pool": pool,
    }

    def decorate(fn: DecoratedFn) -> dg.AssetsDefinition:
        asset_name: str = name or fn.__name__
        prefix: list[str] = []
        if key_prefix is not None:
            prefix = [key_prefix] if isinstance(key_prefix, str) else list(key_prefix)
        key = dg.AssetKey([*prefix, asset_name])

        # Read off the decorated function, so `compute` can hand the context back to a function that asked for one and withhold it from one that did not. Upstream's own rule, so the wrapper and Dagster cannot disagree about one function.
        parameters = list(inspect.signature(fn).parameters.values())
        declares_context: bool = is_context_provided(parameters)

        def reported(
            returned: DecoratedReturn, writer: QuarantineWriter | None
        ) -> AssetYield:
            """Validate what the decorated function handed back and report it.

            One stage either side of `process`, which neither of them changes (#77).
            """
            frame, returned_result = unwrap(returned, asset=key.to_user_string())
            yield from fold(
                process(
                    schema,
                    frame,
                    valid_key=key,
                    quarantine_writer=writer,
                    check_granularity=granularity,
                    multi_column_rules=multi_column,
                    max_failure_samples=failure_samples,
                    statistics=emit_statistics,
                    row_sample=sampled_rows,
                    temp_dir=staging_dir,
                ),
                returned_result,
                valid_key=key,
            )

        if quarantine:
            # The context is the one thing here that a wrapper cannot ask Dagster for after the fact, and building the writer is the only reason to want it (ADR-0001). So a quarantined asset declares it whether or not the decorated function did, and an asset without a quarantine keeps exactly the signature it was written with.

            @functools.wraps(fn)
            def compute(
                context: dg.AssetExecutionContext, *args: object, **kwargs: object
            ) -> AssetYield:
                # Built before the body runs, so a deployment with nowhere to write finds out on its first run rather than on the first one with a failing row.
                writer = _quarantine_writer(context, quarantine_dir=quarantine_root)
                returned: DecoratedReturn = (
                    fn(context, *args, **kwargs)
                    if declares_context
                    else fn(*args, **kwargs)
                )
                yield from reported(returned, writer)

            if not declares_context:
                # `functools.wraps` forwards the decorated function's own signature, which is what Dagster resolves the asset's inputs from. Prepending the context is the whole edit; everything after it is the decorated function's parameter list, untouched.
                compute.__signature__ = inspect.Signature(  # pyrefly: ignore[missing-attribute]
                    [
                        inspect.Parameter(
                            "context",
                            inspect.Parameter.POSITIONAL_OR_KEYWORD,
                            annotation=dg.AssetExecutionContext,
                        ),
                        *parameters,
                    ]
                )
        else:

            @functools.wraps(fn)
            def compute(*args: object, **kwargs: object) -> AssetYield:
                yield from reported(fn(*args, **kwargs), None)

        return dg.asset(
            name=asset_name,
            key_prefix=prefix or None,
            # The column-schema check, both abort paths and the skip all end the step without yielding.
            output_required=False,
            # The package's own key is applied last, so a user cannot accidentally displace the Columns tab.
            metadata={**(metadata or {}), **schema_metadata(schema)},
            check_specs=check_specs(
                schema,
                asset=key,
                check_granularity=granularity,
                multi_column_rules=multi_column,
            ),
            **forwarded,
        )(compute)

    return decorate
