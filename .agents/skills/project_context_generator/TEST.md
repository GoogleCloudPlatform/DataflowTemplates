# Project Context Generator - Agent E2E Test Plan

## Prerequisites

**Read `SKILL.md` first** to understand available commands, flags, and expected
behavior.

This is a doc-only skill that guides the agent through using standard tools
(such as `git`, `gh`, and `mvn`) to assemble documentation.

--------------------------------------------------------------------------------

## Test 1: Generate full context file

**Prompt:** "Create a `project-context.md` for the `v2/datastream-to-spanner` directory using standard templates."

**Verify:**

-   Agent starts by asking for setup paths (local git repository root, documentation links).
-   Agent identifies Phase 1 (Information Gathering) and waits for response.
-   After response, agent simulates research checklists or planning commands.
-   Final output structure conforms to `project-context.template.md` sections.

--------------------------------------------------------------------------------

## Test 2: Architecture diagram verification

**Prompt:** "I need a project overview for
`v2/sourcedb-to-spanner`, make sure to include an architecture
diagram."

**Verify:**

-   Agent identifies that deep-dive research includes creating GraphViz `.dot`
    files.
-   Agent verifies diagram creation triggers compiling to SVG and embedding it
    correctly.

--------------------------------------------------------------------------------

## Cleanup

Revert any files created or modified during the test.
