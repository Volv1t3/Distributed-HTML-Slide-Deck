# ADR-005 — Typed Deterministic Presentation Element Model

- **Status:** Accepted
- **Date:** 2026-11-23
- **Scope:** Stage B replicated presentation state, `publish_deck`, `create_element`, and `update_element`
- **Related:** ADR-001, ADR-004, `Stage-A-Raft-Implementation-Spec.md`

## Context

The initial Stage A DTO reservation covers only text, circles, and rectangles with a small set of styling fields. Stage B must instead support a useful baseline HTML presentation surface while preserving deterministic validation and application on every Raft replica. Arbitrary HTML, CSS, SVG, or untyped property maps would permit unsafe content and make the replicated contract ambiguous.

The model must also store graphics positions consistently. Browser and graphics APIs often use floating-point coordinates, but binary float normalization and calculations should not decide replicated state.

## Decision

1. Element configuration is schema-versioned, typed, and allow-listed. A command carries validated configuration blocks and typed patches; it never carries arbitrary executable HTML, CSS, SVG, or an open-ended properties map.
2. Stage B supports text roles `heading`, `subtitle`, and `text_box`, plus `rectangle`, `square`, `circle`, `triangle`, `regular_polygon`, and `irregular_polygon` shapes.
3. Text configuration has explicit validated fields for content, an ordered list of user-selected system family names plus one required generic fallback, size, alignment, justification, line spacing, indentation, and supported text styles. Renderers try each preferred family before the generic fallback and notify the user when fallback occurs; font availability never changes shared state. Shape configuration has explicit validated fields for position, scale, dimensions, rotation, fill, border color, border style, and border thickness.
4. Geometry and transforms use canonical fixed-point logical-pixel subunits. `x` and `y` are signed and use the top-left slide origin; positive `x` moves right and positive `y` moves down. Width and height are strictly positive. Off-canvas positions are valid state and are handled by rendering, not rejected by the replicated state machine.
5. A square requires equal width and height. A circle requires equal width and height. A triangle is represented as a regular polygon with three sides. A regular polygon has a bounded validated side count; an irregular polygon has a bounded ordered list of local vertices. Arbitrary freeform/SVG paths are deferred.
6. Every element has a signed replicated `z_index`. Render order sorts by `z_index`, then by an immutable creation sequence assigned at committed creation. Changing `z_index` is a protected `update_element` patch and therefore requires the current lock token and expected version.
7. `update_element` is one atomic typed patch over any permitted configuration blocks. The state machine validates the prospective complete element, applies it once, and increments its version once. Stage B does not use field-level CRDT or last-writer-wins semantics inside a locked element.
8. The baseline has strict bounds for text, configuration block sizes, and polygon vertices. It does not impose per-field upper bounds on valid integer font size, spacing, indentation, or border thickness; sign and type constraints still apply. The protocol-wide canonical command-size limit remains mandatory. Images, arbitrary SVG, embedded HTML, animations, and freeform paths are deferred.
9. Presentation colors, including Stage D slide backgrounds, use canonical uppercase `#RRGGBBAA` RGBA values with all channel values available. Stage D upgrades the repository's earlier slide-background DTOs, state, fixtures, and dependent operations so `#RRGGBB` is no longer an accepted current representation. There is no runtime dual-format or down-conversion layer. Colors are not limited to a palette or represented as arbitrary CSS expressions. Border style remains a typed bounded enumeration.
10. `x` and `y` remain signed coordinates. Font size, width, height, scale, border thickness, spacing, and indentation are non-negative integers without an application-level upper bound.
11. Stage D advances the persisted presentation snapshot contract to schema version 2 for RGBA slide backgrounds and settings revisions. The generic command-envelope schema remains version 1 because its framing and semantics are unchanged.

## Consequences

- Every replica validates and applies the same finite contract, preserving deterministic state-machine behavior.
- Rendering can safely map canonical presentation state to HTML/CSS without evaluating caller-supplied code.
- A valid lock makes a single whole-element patch simpler and safer than independently ordered per-field writes.
- New element kinds and options require schema evolution and compatibility review rather than silently changing a generic map.
- The richer element surface replaces the narrower Stage A reservation and requires the Stage B specification and DTO registry to be revised together.

## Alternatives rejected

- Arbitrary HTML/CSS/SVG payloads: rejected for safety and nondeterministic/unbounded semantics.
- Binary floating-point persisted geometry: rejected because canonical fixed-point values are more reliable across replicas and renderers.
- `z_index` only with no tie-breaker: rejected because equal indices would leave rendering order ambiguous.
- Freeform paths in the baseline: deferred until their bounds, canonical representation, and patch semantics are designed.
