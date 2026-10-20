# Domain Docs

This page sets how the engineering skills read this repo's domain documentation when they explore the codebase.

This repo is **single-context**: one `CONTEXT.md` and one `docs/pre-1.0.md` at the root.

## Before exploring, read these

These two files record the project's words and its settled decisions.

- **`CONTEXT.md`** lists the word for each thing.
- **`docs/pre-1.0.md`** records the decisions and the measurements made before the record closed, with each decision under its ADR number.
  Read the entries that touch the area you're about to work in.
  A later decision is in the module that implements it.
- **`docs/out-of-scope/`** has one file per rejected concept, with the measurements that justify the rejection.
  Read it before you propose a feature, so you do not re-derive a settled question.

The `/domain-modeling` skill updates `CONTEXT.md` when a term is resolved.
`/grill-with-docs` and `/improve-codebase-architecture` both use it.
Do not create `docs/adr/` for a new decision: write the decision in the module that implements it, and write a declined design in `docs/out-of-scope/`.

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

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as `CONTEXT.md` defines it.
Do not use a synonym that the glossary lists under _Avoid_.

If the concept you need isn't in the glossary yet, either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag conflicts with a recorded decision

If your output contradicts an ADR in `docs/pre-1.0.md`, a reason in a module's docstring, or a file in `docs/out-of-scope/`, say so explicitly instead of overriding it:

> _Contradicts ADR-0007 in `docs/pre-1.0.md` (the error-rows key is checked at run time), but worth reopening because…_
