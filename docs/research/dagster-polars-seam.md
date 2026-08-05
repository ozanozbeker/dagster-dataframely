# Is there a seam in `dagster-polars` for a second validation library?

Research for [issue #2](https://github.com/ozanozbeker/dagster-dataframely/issues/2).

Sources verified against `dagster-io/community-integrations` at commit
[`5b11a97`](https://github.com/dagster-io/community-integrations/commit/5b11a97949bd23e34df1837e333730c9b3e3ee5e)
(`main`, checked 2026-08-04).
The four files that matter (`dagster_polars/__init__.py`, `dagster_polars/patito.py`, `dagster_polars/io_managers/base.py`, `dagster_polars/io_managers/type_routers.py`) are **byte-identical** between `main` and the PyPI release `dagster-polars==0.27.12`, so every `file:line` citation below is valid for both.
Runtime probes were run in a throwaway venv (`dagster 1.13.16`, `dagster-polars 0.27.12`, `dataframely 3.0.0`, `polars 1.43.2`, `patito 0.8.6`); the repo's `.venv` was not touched.

---

## Answer

**No documented seam exists.
There is a usable *de-facto* seam, and it is mutable global state.**

Concretely:

1. **Where patito attaches: two places, both hard-coded.**
   - A `PatitoTypeRouter` class defined *inside* `dagster-polars` and appended
     to a module-level list `TYPE_ROUTERS` behind
     `importlib.util.find_spec("patito")`
     ([`type_routers.py:164-238`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/io_managers/type_routers.py#L164-L238)).
   - A `_get_patito_metadata` method hard-coded into the base IO manager's
     `get_metadata`
     ([`base.py:202-232`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/io_managers/base.py#L202-L232)).
   - Plus an optional standalone `DagsterType` factory,
     `patito_model_to_dagster_type`
     ([`patito.py:53-110`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/patito.py#L53-L110)),
     which is IO-manager-agnostic and is the only patito symbol in the public
     API docs.

2. **The `TYPE_ROUTERS` list is a de-facto extension point — verified working.**
   It is a plain module-level `list` and `resolve_type_router` iterates it at call time ([`type_routers.py:241-250`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/io_managers/type_routers.py#L241-L250)).
   I appended a third-party router subclassing `BaseTypeRouter` and materialized a two-asset graph through `PolarsParquetIOManager` end to end with dataframely validation on dump — it works (probe below).
   But: the module is not exported from `dagster_polars/__init__.py`, not in the API reference, has zero test coverage for third-party registration, and registration is import-order-dependent mutation of a global.

3. **There is no plugin/entry-point mechanism at all.** `grep` for
   `entry_point`, `importlib.metadata`, `plugin` across the whole
   `dagster_polars` package returns nothing, and `pyproject.toml` declares no
   `[project.entry-points]`.

4. **The maintainer has said the seam should exist and does not yet.**
   On [community-integrations#202](https://github.com/dagster-io/community-integrations/issues/202), `danielgafni` (dagster-polars author):
   > "this error above could be solved if we exposed the type routers at the IOManager constructor argument (we were talking about it previously).
   > Seems like we should actually do it"

   That is an open invitation, and it is the shape an upstream `dagster-polars[dataframely]` PR would most plausibly ride on.

5. **The blocking problem for dataframely is upstream in Dagster, not in dagster-polars.**
   Patito's `Model.DataFrame` is a real runtime class that real instances belong to.
   Dataframely's `dy.DataFrame[S]` is a `typing._GenericAlias` that Dagster rejects outright, and `dy.DataFrame` itself is a phantom type that is **never instantiated** — dataframely's own docstring says so.
   So the patito route (annotate with the typed frame, let Dagster infer the `DagsterType`) is *structurally unavailable* to dataframely, independent of any seam.
   Details in §4.

**Implication for the North Star.**
Upstreaming as `dagster-polars[dataframely]` means proposing the seam, not consuming one.
The extra-name half of the packaging story is trivial and precedented; the mechanism half is not.
And even with a perfect seam, dataframely cannot copy patito's annotation UX without either a synthetic runtime marker class or a fix to
[dagster#22694](https://github.com/dagster-io/dagster/issues/22694).

---

## 1. Where patito attaches

Two hard-coded attachment points, neither exported nor documented.

### 1a. `PatitoTypeRouter` — inside dagster-polars, gated on `find_spec`

[`type_routers.py:227-238`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/io_managers/type_routers.py#L227-L238):

```python
# Order matters!
TYPE_ROUTERS = [
    TypeRouter,
    OptionalTypeRouter,
    DictTypeRouter,
]

if importlib.util.find_spec("patito") is not None:
    TYPE_ROUTERS.append(PatitoTypeRouter)


TYPE_ROUTERS.append(PolarsTypeRouter)
```

Verified at runtime: with patito installed, `TYPE_ROUTERS` is `['TypeRouter', 'OptionalTypeRouter', 'DictTypeRouter', 'PatitoTypeRouter', 'PolarsTypeRouter']`.

`PatitoTypeRouter` ([`type_routers.py:164-224`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/io_managers/type_routers.py#L164-L224)) matches on `issubclass(typing_type, pt.DataFrame)`, recovers the model via `self.typing_type.model`, validates in `dump` and `load`, and declares `inner_type` as plain `pl.DataFrame`/`pl.LazyFrame` so the parent `PolarsTypeRouter` does the actual IO.

`BaseTypeRouter` ([`type_routers.py:37-80`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/io_managers/type_routers.py#L37-L80)) is a documented-by-docstring abstract base with `match`, `is_base_type`, `inner_type`, `dump`, `load`.
Structurally it *is* the right abstraction for a second validation library.
It just isn't offered as one.

### 1b. Metadata — hard-coded into the base IO manager

[`base.py:202-232`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/io_managers/base.py#L202-L232):

```python
def get_metadata(self, context, obj) -> dict[str, MetadataValue]:
    ...
    metadata = get_polars_metadata(context, obj)
    metadata.update(self._get_patito_metadata(context))
    ...


def _get_patito_metadata(self, context: OutputContext) -> dict[str, MetadataValue]:
    # this only returns a non-empty dict if Patito is installed and a Patito model is used as type annotation
    try:
        import patito as pt
        from dagster_polars.patito import get_patito_metadata

        if context.dagster_type.typing_type is not None and issubclass(
            context.dagster_type.typing_type, pt.DataFrame
        ):
            return get_patito_metadata(context.dagster_type.typing_type.model)
    except (ImportError, TypeError):
        return {}
    return {}
```

This is not a hook.
It is `import patito` by name in the base class of every `dagster-polars` IO manager.
A `TypeRouter` has **no** metadata callback — the only way a third party emits `dagster/column_schema` through this path is to subclass every concrete IO manager and override `get_metadata`. (Or to attach metadata to the `DagsterType` instead, as `patito_model_to_dagster_type` also does — see §1c.)

Corroborating evidence that this hard-coding bites in practice: on
[community-integrations#202](https://github.com/dagster-io/community-integrations/issues/202)
a user reports that the patito-derived `dagster/column_schema` overwrites a pandera-supplied one, and asks for a way to disable it.
There is no such way.

### 1c. `patito_model_to_dagster_type` — the only public patito symbol

[`patito.py:53-110`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/patito.py#L53-L110)
builds a `DagsterType` with a patito-validating `type_check_fn`, `metadata=get_patito_metadata(model)`, and `typing_type=model.DataFrame`.
Its docstring says "Compatible with any IOManager" — this route does not require `dagster-polars` IO managers at all, and `dagster_polars_tests/test_patito.py::test_dagster_type_with_default_io_manager` exercises exactly that.

It then does this ([`patito.py:105-108`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/patito.py#L105-L108)):

```python
# this is a dirty hack --- this configures dagster-polars IOManager to skip data validation
# as it is already performed by the DagsterType. We should work on bringing this functionality
# into DagsterType itself
setattr(dagster_type, HANDLES_DATA_VALIDATION_ATTRIBUTE, True)
```

`HANDLES_DATA_VALIDATION_ATTRIBUTE = "_handles_data_validation"`
([`patito.py:50`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/patito.py#L50)).
The name reads generic, but it is defined in the patito module and read in exactly one place — `PatitoTypeRouter.requires_data_validation`
([`type_routers.py:180-185`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/io_managers/type_routers.py#L180-L185)).
It is not a cross-library de-duplication protocol; a dataframely router would have to read the same private attribute deliberately, or invent its own.
Note also that the maintainer's own comment calls it a hack and points at Dagster core as the right home.

### What is *not* a seam

- **No entry points.**
  No `[project.entry-points]` in [`pyproject.toml`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/pyproject.toml); no `importlib.metadata` / `entry_point` / `plugin` anywhere in the package (verified by grep over the installed 0.27.12 tree).
- **No IO-manager constructor argument.** `BasePolarsUPathIOManager` declares exactly two pydantic fields, `base_dir` and `cloud_storage_options` ([`base.py:64-72`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/io_managers/base.py#L64-L72)), and no subclass adds a routers field.
  This is precisely the argument `danielgafni` says should exist (#202).
- **No public export.** `dagster_polars/__init__.py`'s `__all__` is
  `BasePolarsUPathIOManager`, `DataFramePartitions`, `LazyFramePartitions`,
  `PolarsParquetIOManager`, `__version__`, plus `DeltaWriteMode` /
  `PolarsDeltaIOManager` / `PolarsBigQueryIOManager` /
  `PolarsBigQueryTypeHandler` under `try: ... except ImportError`
  ([`__init__.py:1-37`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/__init__.py#L1-L37)).
  Neither `type_routers` nor anything from it is exported.
- **No documentation.**
  The Sphinx page (`docs/sphinx/sections/integrations/libraries/polars/dagster-polars.rst` in `dagster-io/dagster`) documents four IO managers and `dagster_polars.patito.patito_model_to_dagster_type` — and nothing else.
  Its "Supported type annotations" table lists only `DataFrame`, `LazyFrame`, their `Optional` forms, and `Dict[str, ...]`; patito is not in the table.
  <https://docs.dagster.io/api/libraries/dagster-polars>
- **No tests.**
  GitHub code search for `type_routers` across the whole `community-integrations` repo returns exactly two files: `type_routers.py` itself and `base.py`.
  Nothing in `dagster_polars_tests/` registers a router.

**Verdict on "generic vs bespoke":** the *class hierarchy* is generic; the *wiring* is bespoke.
`BaseTypeRouter` was clearly designed as an abstraction, but every affordance that would make it a supported extension point — export, docs, tests, non-global registration, a metadata callback — is absent.

---

## 2. `TYPE_ROUTERS` mutation works — verified end to end

Probe (throwaway venv, not this repo's `.venv`): a `DataframelyTypeRouter` appended to `TYPE_ROUTERS`, a per-schema marker class subclassing `pl.DataFrame` used as `typing_type`, and an explicit `DagsterType`:

```python
class DyFrameBase(pl.DataFrame):
    schema_: type[dy.Schema]


class DyTypeRouter(tr.BaseTypeRouter):
    @staticmethod
    def match(context, typing_type):
        return isinstance(typing_type, type) and issubclass(typing_type, DyFrameBase)

    @property
    def is_base_type(self):
        return False

    @property
    def inner_type(self):
        return pl.DataFrame

    def dump(self, obj, path, dump_fn):
        dump_fn(self.context, self.typing_type.schema_.validate(obj, cast=True), path)


tr.TYPE_ROUTERS.append(DyTypeRouter)
```

Result: `dg.materialize([up, down], resources={"io_manager": PolarsParquetIOManager(...)})` → `SUCCESS: True`, with the router's `dump` and `load` both invoked and dataframely validation actually running.
So the de-facto seam is real.

Constraints discovered while making the probe pass — these are load-bearing design constraints for us:

- **`match` only sees `(context, typing_type)`.** `resolve_type_router` calls `router_class.match(context, dagster_type_to_resolve.typing_type)` ([`type_routers.py:247`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/io_managers/type_routers.py#L247)) — the `DagsterType` itself is *not* passed.
  A first attempt that dispatched on a custom attribute of the `DagsterType` (reached via `context.dagster_type`) caused **infinite recursion**: `parent_type_router` re-resolves with a fresh `TypeHintInferredDagsterType(self.inner_type)` ([`type_routers.py:65-68`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/io_managers/type_routers.py#L65-L68)) but `context` is unchanged, so the router matched itself forever (`RecursionError` during `handle_output`).
  **The schema must be recoverable from the `typing_type` object alone.**
  That is the single sharpest constraint the seam imposes on our type design.
- **Appending is safe; the router must not claim to be a base type.** `PolarsTypeRouter.match` is `typing_type in [pl.DataFrame, pl.LazyFrame]` ([`type_routers.py:153-157`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/io_managers/type_routers.py#L153-L157)) — an `==` membership test, not `issubclass` — so a `pl.DataFrame` *subclass* is not shadowed by it and `TYPE_ROUTERS.append(...)` is sufficient.
  But `is_base_type` must be `False`: `type_router_is_eager` asks `issubclass(pl.DataFrame, type_router.typing_type)` (arguments reversed) ([`base.py:126-139`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/io_managers/base.py#L126-L139)), and for a marker subclass `MyFrame(pl.DataFrame)` both `issubclass(pl.DataFrame, MyFrame)` and `issubclass(pl.LazyFrame, MyFrame)` are `False` (verified), which raises `NotImplementedError`.
  Delegating via `inner_type = pl.DataFrame` avoids this.
- **Ordering relative to `OptionalTypeRouter` / `DictTypeRouter` is favourable.**
  Those two sit ahead of everything and unwrap `X | None` and `dict[str, X]` before delegating, so a router appended at the end still gets partitioned and optional cases for free.

Caveat on all of the above: this is behaviour of an unexported module with no tests covering third-party use.
It could change in any release without it being called a breaking change.

---

## 3. Packaging: extras are the easy part

[`pyproject.toml`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/pyproject.toml):

```toml
[project.optional-dependencies]
deltalake = ["deltalake>=0.25.0"]
gcp = ["dagster-gcp>=0.19.5"]
patito = [
    "patito>=0.8.3",
]
```

Confirmed in the published wheel metadata (`dagster_polars-0.27.12.dist-info/METADATA`): `Provides-Extra: patito` / `Requires-Dist: patito>=0.8.3; extra == "patito"`.

So `dagster-polars[dataframely]` would be a one-line addition — `dataframely = ["dataframely>=3.0.0"]` — plus:

- a `dagster_polars/dataframely.py` module (mirroring `patito.py`),
- a `DataframelyTypeRouter` in `type_routers.py` behind
  `importlib.util.find_spec("dataframely")`,
- optionally a `_get_dataframely_metadata` in `base.py`.

Note the discipline the repo already enforces: PR
[#196](https://github.com/dagster-io/community-integrations/pull/196)
("fix `ImportError` with patito and ensure optional deps are not imported at top level") and CHANGELOG entry "Fixed `ImportError` when `patito` is not installed" — i.e. optional-dep imports must be function-local.
`patito.py` follows this (`if TYPE_CHECKING: import patito as pt` at
[`patito.py:15-17`](https://github.com/dagster-io/community-integrations/blob/5b11a97949bd23e34df1837e333730c9b3e3ee5e/libraries/dagster-polars/dagster_polars/patito.py#L15-L17),
runtime imports inside functions).
Any upstream PR must too.

The install docs mirror this shape: `pip install dagster-polars[patito]` (<https://docs.dagster.io/integrations/libraries/patito>), with patito given its own docs page under `integrations/libraries/` linked from the polars page's "Supplementary" section.

---

## 4. The real obstacle: dataframely's typed frames are phantom types

This is the finding that most constrains our design, and it is upstream of any seam question.

**Patito's parametrised frame is a real class.**
Verified:

| annotation | `resolve_dagster_type(...)` result |
| --- | --- |
| `pl.DataFrame` | ok, `typing_type=polars.dataframe.frame.DataFrame` |
| `pt.DataFrame` | ok, `typing_type=patito.polars.DataFrame` |
| `PM.DataFrame` (patito model) | ok, `typing_type=patito.pydantic.PMDataFrame` |
| `dy.DataFrame` | ok, `typing_type=dataframely._typing.DataFrame` |
| `dy.DataFrame[S]` | **`DagsterInvalidDefinitionError: Invalid type: dagster_type must be an instance of DagsterType or a Python type: got dataframely._typing.DataFrame[__main__.S]`** |
| `dy.LazyFrame[S]` | same error |

`PM.DataFrame` is a dynamically generated *class* (`patito.pydantic.PMDataFrame`) carrying `.model`.
`dy.DataFrame[S]` is a `typing._GenericAlias` (`typing.get_origin` → `dataframely._typing.DataFrame`, `typing.get_args` → `(S,)`), and Dagster rejects it.

That is the same wall `danielgafni` hit trying to add pandera first.
From
[community-integrations#201](https://github.com/dagster-io/community-integrations/issues/201):

> "I was going to add Pandera support to dagster-polars initially, but it didn't work out well because Dagster doesn't support generic type hints (and I wanted the integration to work via type hints). […] I don't have anything against Pandera, but I would like to wait for Dagster to support generics for the best UX in `dagster-polars`."

Tracked as [dagster#22694 "Support generic type hints"](https://github.com/dagster-io/dagster/issues/22694) (open; a commenter said "I'm working on this!" but no linked PR), split out of the abandoned draft PR [dagster#22676](https://github.com/dagster-io/dagster/pull/22676) (closed as stale after 365 days of inactivity).
**Patito was chosen over pandera precisely because patito's non-generic `Model.DataFrame` sidesteps this.
Dataframely is on the pandera side of that line.**

**And dataframely is worse than pandera here**, because the unparametrised `dy.DataFrame` is not a usable fallback either:

- `dy.DataFrame` subclasses `pl.DataFrame` (`__mro__` =
  `dataframely._typing.DataFrame → polars.dataframe.frame.DataFrame → Generic → object`),
  but its own docstring states:
  > "This class is merely used for the type system and never actually instantiated.
  > This means that it won't exist at runtime and `isinstance(PoalrsDataFrame, <var>)` will always fail.
  > Accordingly, users should not try to create instances of this class."

  (`dataframely/_typing.py`, dataframely 3.0.0, read from the installed package.)
- Verified: `S.validate(df)` returns a plain `polars.DataFrame`;
  `isinstance(result, dy.DataFrame)` is `False`.
- Consequence, verified by materialization: annotating an asset
  `-> dy.DataFrame` fails **before the IO manager is reached**, at Dagster's own
  output type check:
  `DagsterTypeCheckDidNotPass: ... Value of type <class 'polars.dataframe.frame.DataFrame'> failed type check for Dagster type DataFrame, expected value to be of Python type dataframely._typing.DataFrame.`
- Verified independently: `resolve_type_router` on `dy.DataFrame` raises `RuntimeError: Could not resolve type router` (`dy.DataFrame != pl.DataFrame`, so `PolarsTypeRouter`'s `==` test misses).
  This is byte-for-byte the failure mode reported in [community-integrations#202](https://github.com/dagster-io/community-integrations/issues/202) for pandera-polars.

**So a dataframely integration cannot use dataframely's own typed frames as Dagster annotations at all** — not the parametrised alias (Dagster rejects the type), not the bare class (Dagster's isinstance check rejects the value).
It must supply an explicit `DagsterType`.
Within that, the two viable shapes are:

1. `typing_type=pl.DataFrame` + a validating `type_check_fn`, schema carried on
   the `DagsterType` — routes through the existing `PolarsTypeRouter`, needs no
   seam at all, but gets no IO-manager-level load-side coercion and no
   `TypeRouter` hook (the schema is invisible to `match`).
2. `typing_type=<synthetic per-schema marker class subclassing pl.DataFrame>` — what the §2 probe used.
   Gets full router participation, at the cost of inventing a runtime class dataframely does not provide.

This corroborates the map's existing note that `dy.DataFrame[Schema]` cannot be a return annotation, and sharpens it: the bare `dy.DataFrame` cannot be one either.

---

## 5. Open issues / PRs / maintainer statements

| Ref | State | Relevance |
| --- | --- | --- |
| [community-integrations#201](https://github.com/dagster-io/community-integrations/issues/201) "Why patito over pandera" | open | Maintainer explains patito was chosen because Dagster lacks generic type-hint support; wants to wait for Dagster generics before adding pandera; points users to `dagster-pandera` meanwhile. |
| [community-integrations#202](https://github.com/dagster-io/community-integrations/issues/202) "[dagster-pandera] x [dagster-polars] Could not resolve type router" | open | The exact `resolve_type_router` failure a second validation library hits. Maintainer: exposing type routers as an IO-manager constructor argument "seems like we should actually do it". Also: patito's `dagster/column_schema` overwrites a user-supplied one, with no opt-out. |
| [community-integrations#179](https://github.com/dagster-io/community-integrations/pull/179) "[dagster-polars] add Patito integration" | merged | The original integration PR, ported from dagster core. Review was a one-line approval — no recorded design discussion about generalising the mechanism. |
| [community-integrations#196](https://github.com/dagster-io/community-integrations/pull/196) | merged | Establishes the "optional deps must not be imported at top level" rule. |
| [dagster#22694](https://github.com/dagster-io/dagster/issues/22694) "Support generic type hints" | open | The upstream blocker for `dy.DataFrame[S]`-style annotations. Filed by the dagster-polars maintainer, explicitly motivated by validation-library integration. |
| [dagster#22676](https://github.com/dagster-io/dagster/pull/22676) "Draft: [dagster-polars] Pandera integration" | closed (stale) | The abandoned first attempt at a second validation library. |
| [dagster#23714](https://github.com/dagster-io/dagster/issues/23714) / [dagster#33780](https://github.com/dagster-io/dagster/pull/33780) | open / open (unmerged) | The pandera-side workaround: resolve `typing_type` to `pl.DataFrame` so `PolarsTypeRouter` matches. This is shape (1) from §4, and is the cheapest known route for a third-party library. |

**No mention of dataframely anywhere in `dagster-io/community-integrations`** (GitHub issue/PR search returned zero hits).

---

## Uncertainty ledger

Verified by reading source or by running code:

- Everything in §1, §2, §3, §4 above.
  Every `file:line` was read from the actual file; every runtime claim was executed.
- The `main`-vs-0.27.12 byte-identity of the four relevant files (`diff -q`).

Inferred, not verified:

- **That appending to `TYPE_ROUTERS` is "acceptable" to maintainers.**
  They have never said so.
  It is unexported, undocumented, and untested for that use.
  I verified it *works*; I did not verify it is *sanctioned*.
- **That a `dagster-polars[dataframely]` PR would be welcomed.** #201's "I don't have anything against Pandera, but I would like to wait for Dagster to support generics" is about pandera and reads as a soft deferral of *any* second validation library until dagster#22694 lands.
  Nobody has asked for dataframely upstream.
- **Whether `dy.Collection` could ride the router mechanism.** `BaseTypeRouter.dump` receives a single `UPath` and may ignore `dump_fn` entirely, and `PolarsParquetIOManager.extension` is a `ClassVar[".parquet"]` (`parquet.py:280`) — so a Collection router would be writing a *directory* named `*.parquet`.
  I did not test this; treat it as unexplored.

Not investigated:

- Whether `dagster-polars`'s Delta and BigQuery IO managers interact differently
  with routers (I only probed `PolarsParquetIOManager`).
- The `dagster-patito` name — no such standalone package appears to exist; the
  docs page at `/integrations/libraries/patito` documents
  `dagster-polars[patito]`, not a separate library.
