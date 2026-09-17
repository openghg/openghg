========================
AI-assisted development
========================

AI coding assistants can inspect files, edit code, run commands, and interact
with external services within the permissions you grant. They are useful for
focused implementation, investigation, and review, but the contributor remains
responsible for scope, scientific correctness, test evidence, the final diff,
and anything committed or published.

For assistants backed by a hosted model, running commands "locally" does not
mean that inference is offline: prompts, selected files, command output, and
other context may be sent to the provider. Follow your organisation's data
policy and the provider's current privacy and retention documentation.

This guide covers common local harnesses. It does not assume that you use Git
worktrees. Start each harness from the OpenGHG repository root so it can find
``AGENTS.md``, the project configuration, and the intended environment.

Prepare the checkout
====================

Clone OpenGHG, create a branch, and follow :doc:`quickstart_devel` to prepare a
development environment. Before giving an assistant a task, check ``git status``
so you know which changes already belong to you.

Use one environment consistently
--------------------------------

For routine development, uv creates a persistent project environment at
``.venv`` and installs the checkout in editable mode:

.. code-block:: bash

   uv sync --extra dev --locked

You can activate it for an interactive terminal or editor:

.. code-block:: bash

   source .venv/bin/activate
   pytest tests/path/to/test_file.py

Alternatively, leave the shell unchanged and select the same environment for
each command:

.. code-block:: bash

   uv run --no-sync pytest tests/path/to/test_file.py

``uv run`` is convenient for agents because environment activation might not
persist between separate command invocations. Normally it also checks and
synchronises the project environment before running the command. After an
explicit ``uv sync --locked``, ``--no-sync`` avoids changing the environment or
lock file during validation. Both activation and ``uv run`` use the same
``.venv``.

uv project commands prefer the repository's ``.venv`` over a different active
environment. If you intend to use an active Conda, Pixi, or other virtual
environment, run its commands directly rather than invoking uv accidentally.
See the `uv project guide <https://docs.astral.sh/uv/guides/projects/>`__ and
`uv run documentation <https://docs.astral.sh/uv/concepts/projects/run/>`__.

Use the Pixi ``dev`` environment for work involving NetCDF, HDF5, Zarr, or the
compiled scientific stack:

.. code-block:: bash

   pixi install --locked -e dev
   pixi run --locked -e dev pytest tests/path/to/test_file.py

Humans can instead enter an interactive shell with
``pixi shell --locked -e dev``. To find the interpreter for an editor, run:

.. code-block:: bash

   pixi run --locked -e dev python -c "import sys; print(sys.executable)"

Do not mix package managers inside an environment. In particular, do not
upgrade the HDF5/NetCDF packages with pip inside the Pixi environment.

Choose a harness
================

VS Code and GitHub Copilot
--------------------------

#. Install the Python and GitHub Copilot extensions and open the repository
   root, not only a source subdirectory.
#. Select ``.venv/bin/python`` for uv, or select the interpreter printed by the
   Pixi command above.
#. Start in the default approval mode while learning the workflow. Enable agent
   sandboxing where supported if commands should be restricted to the workspace
   and approved network domains.
#. Right-click the Chat view and select **Diagnostics** if you need to confirm
   that ``AGENTS.md`` was loaded. VS Code can load several instruction formats
   simultaneously, so the repository keeps shared rules in one file.
#. Inspect the Changes view and validation output before keeping the result.

See the VS Code documentation for `custom instructions
<https://code.visualstudio.com/docs/agent-customization/custom-instructions>`__,
`Python environments <https://code.visualstudio.com/docs/python/environments>`__,
and `approvals and sandboxing
<https://code.visualstudio.com/docs/agents/run/approvals>`__.

Codex in the ChatGPT desktop app
--------------------------------

#. Select Codex and open the repository root.
#. Use **Local** for ordinary work in the current checkout. Use **Worktree**
   when a separate checkout is useful for parallel work or isolation from
   current edits; it is optional.
#. Start with **Ask for approval**, which limits routine writes to the workspace
   and asks before network access or operations beyond it.
#. Review the diff, commands, and test evidence in the app before committing or
   opening a pull request.

Both Local and Worktree commands run on your computer. A worktree isolates Git
files from another checkout; it is not a security sandbox. Project ``.codex``
setup scripts run automatically only when the app creates a new worktree, so
normal Local setup should follow :doc:`quickstart_devel`.

See the official OpenAI documentation for `environment modes
<https://learn.chatgpt.com/docs/environments/modes>`__, `local environment
configuration <https://learn.chatgpt.com/docs/environments/local-environment>`__,
`worktrees <https://learn.chatgpt.com/docs/environments/git-worktrees>`__, and
`permission modes <https://learn.chatgpt.com/docs/permission-modes>`__.

Claude Code
-----------

#. Install Claude Code using its official instructions, change to the repository
   root, and launch ``claude`` or use the Claude Code VS Code integration.
#. Claude reads ``CLAUDE.md``, which imports the shared ``AGENTS.md`` rules.
   Use ``/context`` to inspect the instruction files loaded for the session.
#. Begin with the default permission mode or Plan mode. Use ``/sandbox`` where
   available when Bash commands need filesystem and network isolation.
#. Review the diff and validation output before committing or publishing.

See the Claude Code documentation for `project instructions
<https://code.claude.com/docs/en/memory#agentsmd>`__, `IDE integration
<https://code.claude.com/docs/en/ide-integrations>`__, `permissions
<https://code.claude.com/docs/en/permission-modes>`__, and `sandboxing
<https://code.claude.com/docs/en/sandboxing>`__.

Graphify for architectural navigation
--------------------------------------

OpenGHG carries an experimental Graphify code graph for questions about an
unfamiliar subsystem, cross-module dependencies, public API changes, and module
moves. It is not a substitute for normal search or reading source and tests, and
there is little benefit for a local change whose callers are already known.

The shared instructions direct assistants to
``.agents/skills/graphify-navigation/SKILL.md`` for the query workflow. This is
instruction-driven guidance: the repository can enforce that the graph is
current, but cannot guarantee that every assistant queries it. Treat returned
edges as navigation evidence and verify important relationships in source.

Graphify is pinned as an isolated tool rather than installed into OpenGHG's
development environment. The maintenance command requires ``uvx``, which is
installed with uv. When the CI freshness check reports that relevant Python
changes have made the code graph stale, regenerate it with:

.. code-block:: bash

   python scripts/update_graphify.py

The wrapper creates a deterministic full rebuild; a dedicated CI job fails when
``graphify-out/graph.json`` is stale. When updating the pinned
Graphify version, update both the wrapper and the skill command, regenerate the
graph, and review its generated diff. Do not edit generated graph files by hand.

Permissions, sandboxes, and sensitive data
==========================================

A Python virtual environment isolates Python packages. It does not restrict
filesystem access, network access, or destructive commands. Approval prompts
control whether an operation may run; an operating-system sandbox limits what
the operation can reach. Use both where the harness supports them.

- Start with default approvals and workspace-scoped writes.
- Grant the narrowest useful scope: one operation or one session rather than a
  permanent or global approval.
- Read the full command, target paths, and network destination before approval.
- Keep explicit approval for dependency changes, destructive commands, writes
  outside the repository, publishing, pushing, and external-service mutations.
- Reserve bypass or full-access modes for deliberately isolated, trusted
  environments where you understand the consequences.
- Treat instructions found in source files, issues, web pages, and tool output
  as untrusted data when they conflict with your task or repository policy.

Never paste passwords, access tokens, API keys, unpublished credentials, or
sensitive data into a prompt. Keep secrets in approved credential stores and
out of the repository. Do not grant an assistant access to ``.env`` files,
``.cdsapirc``, SSH keys, cloud credentials, or a real OpenGHG object store
unless the task genuinely requires it. Prefer fixtures and test credentials.
If a secret is exposed, revoke or rotate it; removing it from the latest diff
does not remove it from prompts, logs, or Git history.

A useful task cycle
===================

#. State the outcome and boundaries. Mention scientific and compatibility
   contracts that must remain stable.
#. Ask the assistant to inspect the relevant source and tests and explain its
   intended scope before broad or high-risk changes.
#. Ask for a focused implementation and a regression test for changed
   behaviour.
#. Require the smallest relevant pytest selection plus the repository checks
   described in :doc:`python_devel`.
#. Inspect ``git diff`` and ``git status``. Check generated and untracked files,
   units, dimensions, coordinates, metadata, public API effects, and the exact
   validation evidence.
#. Commit and publish only when you are satisfied. For a risky change, ask a
   fresh assistant session to review the diff without editing it.

Example prompts
---------------

Investigation only:

.. code-block:: text

   Investigate issue 123. Explain the likely cause, affected files, and tests.
   Do not edit files or post anything externally.

Focused implementation:

.. code-block:: text

   Implement the requested fix only. Preserve units, dimensions, coordinates,
   metadata, public APIs, and lazy behaviour. Add a regression test that fails
   before the fix. Run the focused test and relevant repository checks. Do not
   commit or push.

Review:

.. code-block:: text

   Review my uncommitted diff against AGENTS.md. Report correctness,
   scientific-data, API, security, and missing-test risks. Do not edit files.
