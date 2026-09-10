"""The decorator: one argument attaches a Dataframely schema to a Dagster asset.

The decorator coordinates three artifacts no single `@dg.asset` parameter accepts as a bundle: the check specs, the definition metadata, and the wrapped runtime. `@dbt_assets` is the first-party precedent.

ADR-0005 has why the decorator wraps `dg.asset` rather than stacking under it: a check spec is an op output, and no public API adds one to a finished `AssetsDefinition` or swaps its compute function.

This module carries no `from __future__ import annotations`. At a 3.12 floor it would buy only unquoted forward references, and it would turn user-facing annotations into strings that Dagster's runtime introspection rejects. The cost is that typing-only names such as `dg.CoercibleToAssetDep` are absent at runtime, so they are spelled here with runtime-real types.
"""

import functools
import inspect
from collections.abc import Callable, Iterable, Mapping, Sequence
from collections.abc import Set as AbstractSet
from typing import Any

import dagster as dg
import dataframely as dy
import polars as pl

# Upstream's own rule for whether a decorated function asked for a context.
from dagster._core.definitions.decorators.op_decorator import is_context_provided

# Raised by every context property only a real step can answer. The wrapper uses it to
# tell a run from a direct invocation. Characterization tests pin both imports.
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
    Granularity,
    MultiColumnRules,
)
from dagster_dataframely.errors import (
    CollectionNotSupportedError,
    QuarantineDirError,
)

DecoratedFn = Callable[..., DecoratedReturn]
"""What `dy_asset` accepts. The return union makes a function returning anything else a static error, which `tests/test_asset_runtime.py` pins with a `pyrefly: ignore` on the call that breaks it."""

AutomationCondition = (
    dg.AutomationCondition[dg.AssetKey]
    | dg.AutomationCondition[dg.AssetKey | dg.AssetCheckKey]
)
"""The union `@dg.asset` accepts, spelled out because `AutomationCondition` is generic and its two parameterizations are not interchangeable."""

AssetDep = (
    dg.AssetKey
    | str
    | Sequence[str]
    | dg.AssetSpec
    | dg.AssetsDefinition
    | dg.SourceAsset
    | dg.AssetDep
)
"""Runtime-real spelling of Dagster's `CoercibleToAssetDep`, which is typing-only."""


def _chosen_writer(context: dg.AssetExecutionContext) -> QuarantineWriter:
    """Choose who writes this asset's invalid rows.

    Delegation first. The asset's own IO manager puts the rows wherever it puts things, which needs no configuration and cannot disagree with where the valid table went (ADR-0006).

    `file_writer` answers the one case with no step: direct invocation, where the asset is called rather than run, so Dagster builds no step for `delegating_writer` to borrow the output context and IO manager from. A test that wants the real placement runs the asset.

    The step is asked for on its own, ahead of the writer. No predicate answers "is this a run", so the question has to be a call that raises. Wrapping the whole of `delegating_writer` in that guard would widen it: any other property raising the same error inside a real run would silently reroute the rows to a file.

    `quarantine_dir` is resolved behind the same guard, so a run reads a setting it has no use for on no path at all.

    Parameters
    ----------
    context
        The executing asset's context.

    Returns
    -------
    The writer for the rows in hand.

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
    quarantine_dir: str | None = QUARANTINE_DIR.resolve(None)
    if quarantine_dir is None:
        raise QuarantineDirError(context.asset_key.to_user_string())
    return file_writer(
        context.asset_key,
        quarantine_dir,
        # Read behind the guard because `partition_key` raises on an unpartitioned asset rather than answering `None`.
        context.partition_key if context.has_partition_key else None,
    )


def _quarantine_writer(context: dg.AssetExecutionContext) -> QuarantineWriter:
    """Build the writer that picks its route when the invalid rows arrive.

    The route is a property of the rows, not of the declaration, so nothing about it is decided until there are rows (#115). Two things follow. A call whose every row is valid never asks where invalid ones would go, so it needs no quarantine_dir for rows that do not exist. And a deployment that sets `DAGSTER_DATAFRAMELY_QUARANTINE_DIR` after the module holding the asset imported is read, not ignored, which is what a test pointing the variable at a `tmp_path` does.

    Deferring costs a run nothing. `process` calls a writer once, so the choice runs at most once either way, and a run reaches the same `delegating_writer` it always did.

    Parameters
    ----------
    context
        The executing asset's context, held until the rows come.

    Returns
    -------
    The writer to hand `process`.
    """

    def write(frame: pl.DataFrame) -> str:
        return _chosen_writer(context)(frame)

    return write


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
    # --- `@dg.asset`'s own, forwarded. ---
    name: str | None = None,
    key_prefix: str | Sequence[str] | None = None,
    ins: Mapping[str, dg.AssetIn] | None = None,
    deps: Iterable[AssetDep] | None = None,
    metadata: Mapping[str, Any] | None = None,
    tags: Mapping[str, str] | None = None,
    description: str | None = None,
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

    With the `dy.Schema` in place, the asset's catalog Columns tab fills in before the asset has run, every Dataframely rule reports through an asset check with pass/fail history (one check per rule unless `check_granularity` collapses them), and a frame whose column schema does not match the schema aborts the run before a row is filtered.

    The decorated function keeps plain Polars annotations. Nothing rewrites the signature, upstream dependencies bind as ordinary parameters, and a declared `context` binds as it does on a plain `@dg.asset`. Five returns are accepted: a frame or a `dg.MaterializeResult` carrying one, eager or lazy, or `None`:

    ```text
    pl.DataFrame
    dg.MaterializeResult[pl.DataFrame]

    pl.LazyFrame
    dg.MaterializeResult[pl.LazyFrame]

    None
    ```

    The return annotation is not enforced. Like any Python annotation it serves your type checker. What the function returns decides what happens: a `LazyFrame` and a `DataFrame` are validated the same way, so a signature that disagrees with the body changes nothing. `@dg.asset` does hold you to its own annotation at run time, so that expectation does not carry over. Parameterize a returned result when you annotate one, since a bare `dg.MaterializeResult` is an implicit `Any` a strict checker rejects.

    Returning `None` skips the asset. Nothing is validated, nothing materializes, and the run succeeds. It is for the partition that has no data and never will:

    ```python
    def sales(context: dg.AssetExecutionContext) -> pl.DataFrame | None:
        path = source_path(context.partition_key)
        if not path.exists():
            return None
        return pl.read_parquet(path)
    ```

    Deciding there is no data is your job, which the `path.exists()` call above does. The decorator catches nothing to decide it for you: it cannot tell a file that is legitimately absent from a path someone misconfigured, and guessing wrong would turn a broken pipeline into a silently missing partition. Return `None` when the data will never arrive, and let an error escape when something is wrong.

    Every check still reports on a skipped run, and passes. A check spec is a non-optional output whatever the asset declares, so a step that answers none of them fails outright. The rules therefore run over an empty frame: each rule was evaluated over zero rows and none was violated. Dagster attaches those evaluations to no materialization, so a passing check on a skipped partition does not claim to have checked an earlier one.

    `@dg.asset` accepts a returned result, and it is the only route to a materialization's tags and data version:

    ```python
    def orders(raw_orders: pl.DataFrame) -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(value=raw_orders, metadata={"source": "stripe"})
    ```

    Its `value` is the frame to validate, and is required. `dg.MaterializeResult(value=None)` is refused rather than read as the skip: a returned result exists to put something on a materialization, and a skipped run has none. Its `metadata`, `data_version` and `tags` land on the materialization; the package's own metadata keys win a collision, as they do for `metadata=`. `asset_key` and `check_results` are refused by name, because the decorator decides both.

    A returned result is the route this package prefers; the context is the other one. `context.add_asset_metadata({...})` reaches the same materialization. It overrides this package's own metadata keys, where a returned result loses to them, and it cannot be reached by calling the asset.

    The asset's declaration is the failure policy. There is no lenient mode and no strict flag. `quarantine=True` is the consent to partial data, so what an invalid row costs is visible in the definition and cannot disagree with what the asset declares. The skip sits outside this: it says there were no rows to have a policy about.

    With no quarantine, every row has to be valid. A run with one failing row writes nothing and leaves the last-known-good table in place. To drop rows anyway, filter in the asset body, where the drop is a line you wrote:

        valid, _ = Orders.filter(raw_orders)
        return valid

    With a quarantine, the invalid rows go to the asset's own IO manager under the key `<name>_quarantine`, carrying the original columns plus a rule column for every rule. They land wherever that manager puts things: a parquet file beside the table under `dagster-polars`, a second table beside it under `dagster-duckdb-polars`. The checks then fail at `WARN` and the run succeeds, so downstream proceeds on the data that is fine. The materialization records where the rows went, how many there were, which sets of rules they failed together, and a sample. A clean run writes no quarantine. A run where nothing survived writes the quarantine and skips the asset rather than emptying it.

    Every parameter is declared with its runtime-real type, so editors autocomplete them and `group_nme="sales"` is a static error rather than an import-time crash. `check_specs` is absent because this decorator owns it.

    Parameters
    ----------
    schema
        The Dataframely schema the decorated function's output must satisfy. Positional-only, and the only such parameter: it is the reason this decorator exists, so it is required and never one keyword among thirty.
    quarantine
        Whether invalid rows are kept. `True` is the consent to partial data, `False` the refusal. It needs no configuration: the rows go wherever this asset's manager puts things. **`True` adds a `context` parameter** to the asset whether or not the decorated function declared one, because the writer is built from the execution context. Calling a quarantined asset directly therefore takes a `dg.build_asset_context()` first. A direct call reaches no IO manager, so the rows go to a parquet file under `DAGSTER_DATAFRAMELY_QUARANTINE_DIR`, read when there are rows to write and raising then if it is unset. A call that holds nothing back needs no directory.
    check_granularity
        How far the schema's rules collapse into checks. `rule` gives each rule its own check and its own history. `column` gives one check per rule-bearing column, `dy_col__<column>`, which keeps a wide schema's check list readable. `schema` gives a single `dy_schema__rules` for all of them. **Changing this on an existing asset orphans check history**: the old check names stop being reported and their histories end where the change landed, while the new ones start empty. Nothing migrates them, so choose it before the asset ships. Unset resolves through `DAGSTER_DATAFRAMELY_CHECK_GRANULARITY`, then the package default `rule`.
    multi_column_rules
        Where the rules no single column owns land at `column` granularity: grouped into `dy_schema__rules`, or `per_rule` for a check each. Read at no other granularity, because neither has a second place to put them. Unset resolves through `DAGSTER_DATAFRAMELY_MULTI_COLUMN_RULES`, then the package default `schema`.
    max_failure_samples
        How many rows that failed a rule reach that rule's check metadata, under `dy_failed_sample`. It answers what a failing check and its counts cannot, so it is opt-out and `0` turns it off. **These are real rows in the Dagster event log**, which is shared, exported and not redacted. The bound is this package's own; `dy.Config.set_max_failure_examples` does not touch it. Bounded per rule, so a collapsed check shows this many for each rule anything failed. Unset resolves through `DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES`, then the package default `5`.
    statistics
        Whether the materialization carries `skimr`-style statistics for what it wrote: one table per dtype family present. Opt-out, so `False` turns the pass off. The string family carries no value-bearing statistic at either value, only lengths and cardinality, because consenting to summary statistics is not consenting to raw values. Raw values are the two sample settings' business, so they stay separate from this one. The quarantine carries none at any value, because nothing consumes it. Unset resolves through `DAGSTER_DATAFRAMELY_STATISTICS`, then the package default `true`.
    row_sample
        How many rows reach the materialization metadata, of what was written under `dataframely/valid_sample` and of what was held back under `dataframely/invalid_sample`. Opt-out on the same terms as `max_failure_samples`, with the same consequence: **these are real rows in the event log**, and `0` turns them off. One number for both, so consenting to a sample is one decision. Unset resolves through `DAGSTER_DATAFRAMELY_ROW_SAMPLE`, then the package default `5`.
    name
        Asset name. Defaults to the function name.
    key_prefix
        Prefix for the asset key. The checks and the quarantine follow it.
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
        Run configuration schema for the underlying op. Narrower than Dagster's six-member union: a mapping is the spelling worth a static guarantee, and the rest are legacy.
    required_resource_keys
        Resources the decorated function reaches through the context. The quarantine needs none: its manager comes off the step, not off the context's resources.
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
    # Only a `Collection` is refused here. Anything else keeps failing however it already fails.
    if isinstance(schema, type) and issubclass(schema, dy.Collection):
        raise CollectionNotSupportedError(schema.__name__)

    # Resolved once, here, and handed to both the specs and the runtime. Resolving again inside the run would read the executing process's environment, so a worker with a different `DAGSTER_DATAFRAMELY_*` would report against checks the code location never declared. The last three affect nothing built at definition time, but they resolve here too: a mistyped environment variable then fails where the asset is declared rather than on whichever run reaches it first.
    granularity: Granularity = CHECK_GRANULARITY.resolve(check_granularity)
    multi_column: MultiColumnRules = MULTI_COLUMN_RULES.resolve(multi_column_rules)
    failure_samples: int = MAX_FAILURE_SAMPLES.resolve(max_failure_samples)
    emit_statistics: bool = STATISTICS.resolve(statistics)
    sampled_rows: int = ROW_SAMPLE.resolve(row_sample)
    # `quarantine_dir` is the one whose value is dropped. What a deployment writes after this module imports is what a call should use, and a call holding nothing back should never have to name a directory, so the writer reads the setting itself when the rows arrive (#115).
    # The resolve stays for the sentence above it: `${SCRATCH}` unexpanded arrives empty, and a variable written wrong is worth reporting where it was written rather than on whichever call first has a row to hold back. Unconditional, because a malformed variable is malformed whether or not this asset declares a quarantine.
    QUARANTINE_DIR.resolve(None)

    forwarded: dict[str, Any] = {
        "ins": ins,
        "deps": deps,
        "tags": tags,
        # The schema's docstring fills a description the decorator was not given, because the schema describes the table while the decorated function's docstring describes the code that fills it. Returning nothing leaves Dagster's own fallback to that docstring standing, so an empty string on either source reads as absent and neither can say "no description at all".
        # `__doc__` rather than `inspect.getdoc`, which walks the MRO: a schema with no docstring would inherit `dy.Schema`'s and describe itself as a base class for schema definitions. `cleandoc` because a raw docstring keeps its source indentation, which the catalog renders as a code block.
        "description": description or inspect.cleandoc(schema.__doc__ or "") or None,
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

        # Read off the decorated function, so `compute` can hand the context to a function that asked for one and withhold it from one that did not.
        parameters = list(inspect.signature(fn).parameters.values())
        declares_context: bool = is_context_provided(parameters)

        def reported(
            returned: DecoratedReturn, writer: QuarantineWriter | None
        ) -> AssetYield:
            """Validate what the decorated function handed back and report it.

            One stage either side of `process`, and neither changes it (#77).
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
                ),
                returned_result,
                valid_key=key,
            )

        if quarantine:
            # The context is the one thing a wrapper cannot ask Dagster for after the fact, and building the writer is the only reason to want it (ADR-0001). So a quarantined asset declares it whether or not the decorated function did, and an asset without a quarantine keeps the signature it was written with.

            @functools.wraps(fn)
            def compute(
                context: dg.AssetExecutionContext, *args: object, **kwargs: object
            ) -> AssetYield:
                # Built before the body runs because the context is in hand here and nowhere else. It picks its route later, when there are rows to write, so a body that holds nothing back needs nowhere to put it.
                writer = _quarantine_writer(context)
                returned: DecoratedReturn = (
                    fn(context, *args, **kwargs)
                    if declares_context
                    else fn(*args, **kwargs)
                )
                yield from reported(returned, writer)

            if not declares_context:
                # `functools.wraps` forwards the decorated function's own signature, which Dagster resolves the asset's inputs from. Prepending the context is the whole edit; the rest is the decorated function's parameter list, untouched.
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
            # The column-schema check, both failures that write nothing, and the skip all end the step without yielding.
            output_required=False,
            # The package's own key is applied last, so a user cannot accidentally displace the Columns tab.
            # The opposite precedence from `description` above: this key belongs to the package, while a description is prose the author owns.
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
