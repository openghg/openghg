---
name: graphify-navigation
description: Use the tracked OpenGHG Graphify graph to orient in an unfamiliar subsystem or trace imports, callers, and likely impact paths. Use for cross-module impact analysis, public-API changes, module moves, and architectural exploration; do not use for localized work where the relevant files and callers are already known.
---

# Navigate with Graphify

1. Confirm that `graphify-out/graph.json` exists. If it is missing, use `rg --files` and `rg`;
   do not generate a graph unless the user asks.
2. Run a scoped query with the pinned CLI, for example
   `uvx --python 3.12 --from graphifyy==0.9.55 graphify query "what depends on <symbol>?"`.
   Use the same `uvx` prefix with the top-level commands `graphify affected "<symbol>"`,
   `graphify explain "<symbol>"`, or `graphify path "<A>" "<B>"` when appropriate. Do not
   load the complete JSON graph into context.
3. Use the result to identify candidate source files, public exports, and test areas.
4. Verify every relevant relationship against source and tests before editing. Treat inferred
   edges and generated summaries as navigation evidence, not source of truth. Graphify does not
   currently classify `.pyi` stubs, so inspect relevant public API stubs directly.
5. If Graphify is unavailable or reports stale output, continue with normal source search and
   report the limitation. Do not edit generated output by hand.

When relevant Python changes make the freshness check fail, run
`python scripts/update_graphify.py` and include the regenerated `graphify-out/graph.json` in
the change.
