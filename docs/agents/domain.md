# Domain Docs

How engineering skills should consume this repository's domain documentation when exploring the codebase.

## Before exploring, read these

- `GLOSSARY.md` at the repository root, when it exists.
- `docs/adrs/`: ADRs that touch the area being changed.

If either location does not exist, proceed silently. Do not create these documents upfront; the domain-modeling workflow creates them when terms or decisions are actually resolved.

## File structure

Single-context repository:

```
/
├── GLOSSARY.md
├── docs/
│   └── adrs/
│       └── ADR-001-element-level-distributed-locking.md
└── src/
```

## Use the glossary's vocabulary

Use terms defined in `GLOSSARY.md` consistently in issue titles, proposals, hypotheses, and tests. If a required concept is missing, reconsider whether the project already uses another term or record the gap for domain modeling.

## Flag ADR conflicts

Surface any conflict with an existing ADR explicitly instead of silently overriding it.
