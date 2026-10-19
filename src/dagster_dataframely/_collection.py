"""`dd.collection_checks`, which reports a Dataframely Collection's collection filters as asset checks on the member assets the user declares.

The checks read the tables those assets wrote, and they evaluate only the collection filters.
They never change a table: `docs/out-of-scope/collection-assets.md` records why a Collection produces no assets.
"""

import inspect
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass

import dagster as dg
import dataframely as dy
import polars as pl

from dagster_dataframely._naming import collection_filter_check_name
from dagster_dataframely._rules import Severity, collection_filter_severities, quoted
from dagster_dataframely._samples import sample_metadata, sample_rows
from dagster_dataframely._settings import MAX_FAILURE_SAMPLES
from dagster_dataframely.errors import FailurePropagationError, InvalidMembersError

type Members = Mapping[str, dg.AssetsDefinition | dg.AssetSpec]


@dataclass(frozen=True)
class _FilterCheck:
    """The asset check for one collection filter on one member."""

    filter_name: str
    member: str
    spec: dg.AssetCheckSpec


def _snake_cased(name: str) -> str:
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
            f"names {quoted(unknown)}, which is not a member of {owner}."
            if len(unknown) == 1
            else f"names {quoted(unknown)}, which are not members of {owner}."
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
            f"lacks the required member{plural} {quoted(missing)} of {owner}.",
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
            f"maps {quoted(several)} to a definition of several assets. Pass the member's `dg.AssetSpec` from its `specs`.",
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
            f"maps {quoted(partitioned)} to {assets}, and the checks read every member's whole table. Pass unpartitioned assets.",
            names,
        )


def _filter_checks(
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
                collection_filter_check_name(filter_name),
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


def collection_checks(
    collection: type[dy.Collection],
    /,
    *,
    members: Members,
    name: str | None = None,
    max_failure_samples: int | None = None,
) -> dg.AssetChecksDefinition:
    """Return an asset check for each collection filter of `collection` on each of its member assets.

    Parameters
    ----------
    members
        The asset for each member of `collection`, keyed by member name.
    name
        The name of the step that runs the checks. `None` uses the Collection's class name in snake case.

    Returns
    -------
    :
        One definition with every check, which Dagster runs in one step.
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
    severities: dict[str, Severity] = collection_filter_severities(collection)
    limit: int = MAX_FAILURE_SAMPLES.resolve(max_failure_samples)
    checks: list[_FilterCheck] = _filter_checks(collection, keys, severities)
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
                # The member's own columns, without the rule columns `details()` adds.
                columns: list[str] = lazy_frames[check.member].collect_schema().names()
                details: pl.DataFrame = failure[check.member].details()
                metadata["dataframely/failed_row_count"] = failed
                metadata |= sample_metadata(
                    "dataframely/failed_sample",
                    sample_rows(
                        details.filter(pl.col(check.filter_name) == "invalid").select(
                            columns
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
                metadata=metadata,
            )

    # Dagster reads each input's name and type off the signature, and the members set both.
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
        name=name or _snake_cased(collection.__name__),
        can_subset=True,
    )(_results)
