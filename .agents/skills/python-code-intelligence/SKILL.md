---
name: python-code-intelligence
description: Navigate Python code with type-aware definitions, references, call hierarchies, and type hierarchies. Use for Python symbol tracing and impact analysis; use Graphify instead for broad architectural orientation or non-Python relationships.
---

# Navigate Python semantically

Use the bundled adapter before broad text search when a Python task asks where a
symbol is defined, referenced, called, inherited, or likely to have impact.

## Check availability

Run this once when the environment is unfamiliar:

```bash
python .agents/skills/python-code-intelligence/scripts/lsp.py doctor
```

Pyright is the default backend. The adapter uses an installed
`pyright-langserver` when available, otherwise it uses `npx` to download a
pinned Pyright release. This requires Node.js and network access on first use.

ty is optional. The adapter uses an installed `ty` executable when available,
otherwise it uses `uvx` to download a pinned release. It is needed only for type
hierarchy queries or an independent semantic cross-check. These tools are not
OpenGHG runtime or development dependencies.

## Query Python symbols

Run commands from the repository root:

```bash
python .agents/skills/python-code-intelligence/scripts/lsp.py search SYMBOL
python .agents/skills/python-code-intelligence/scripts/lsp.py definition SYMBOL
python .agents/skills/python-code-intelligence/scripts/lsp.py references SYMBOL
python .agents/skills/python-code-intelligence/scripts/lsp.py callers SYMBOL
python .agents/skills/python-code-intelligence/scripts/lsp.py callees SYMBOL
python .agents/skills/python-code-intelligence/scripts/lsp.py hover SYMBOL
python .agents/skills/python-code-intelligence/scripts/lsp.py --backend ty supertypes CLASS
python .agents/skills/python-code-intelligence/scripts/lsp.py --backend ty subtypes CLASS
```

Pass a dotted name such as `Datasource.add_timed_data` to disambiguate a member.
If multiple symbols remain, narrow the result with `--path openghg/store`.

Use Pyright for ordinary definition, reference, call, and hover queries. Use ty
for type hierarchies or to check a surprising Pyright result. Do not run both
backends routinely.

## Combine the evidence

- Use Graphify for an unfamiliar subsystem, cross-module architecture, broad
  multi-hop impact, or non-Python relationships, then use this skill to verify
  exact Python bindings against the current checkout.
- Use `rg` for strings, configuration, dynamically resolved names, and other
  relationships a language server cannot see.
- Read the relevant source and tests before deciding scope. A reported reference
  or call edge does not prove runtime reachability.
