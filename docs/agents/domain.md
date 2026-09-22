# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

This repo is **single-context**: one `CONTEXT.md` and one `docs/pre-1.0.md` at the root.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root.
- **`docs/pre-1.0.md`**. It records every decision and measurement the package was built on. Read the entries that touch the area you're about to work in. It is closed at 1.0, so nothing is added to it.
- **`docs/out-of-scope/`** — one file per rejected concept, with the measurements that rejected it. Read before proposing a feature, so a settled question is not re-derived.

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. The `/domain-modeling` skill (reached via `/grill-with-docs` and `/improve-codebase-architecture`) creates them lazily when terms or decisions actually get resolved.

## File structure

```text
/
├── CONTEXT.md
├── docs/pre-1.0.md
├── docs/out-of-scope/
│   └── <concept>.md
└── src/dagster_dataframely/
```

`/triage` looks for rejected concepts in a root `.out-of-scope/` by default.
They live in `docs/out-of-scope/` here instead, beside the decision record, because a rejection record is something a reader goes looking for rather than something to hide in a dotdir.

If this repo ever splits into multiple bounded contexts, add a root `CONTEXT-MAP.md` pointing at one `CONTEXT.md` per context, and give each context its own decision record beside its `CONTEXT.md`.

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal — either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0007 (event-sourced orders) — but worth reopening because…_
