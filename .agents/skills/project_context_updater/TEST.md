# Project Context Updater - Agent E2E Test Plan

## Prerequisites

**Read `SKILL.md` first** to understand available commands, flags, and expected
behavior.

This is a doc-only skill that guides the agent through using standard tools to
update documentation file states.

--------------------------------------------------------------------------------

## Test 1: Update existing context file

**Prompt:** "I have just completed a change improving the build queue execution.
Please update the `project-context.md` file located at
`v2/datastream-to-spanner` to describe these updates."

**Verify:**

-   Agent reads the existing file first to familiarize with the setup.
-   Checklist includes validating build commands / diagrams / tech stack versions / AI tips before writing.
-   Verifications chain with feedback loops before formulating the change list.
-   Agent creates a PR using `gh pr create`.

--------------------------------------------------------------------------------

## Test 2: Project Ramp-Up

**Prompt:** "I'm new to project `v2/sourcedb-to-spanner`, help me get oriented and understand the architecture and tech stack."

**Verify:**

-   Agent reads the local `project-context.md` file to fetch user onboarding
    context.
-   Agent identifies the `AI Agent Tips` listed in the tips section for fast
    workflows.

--------------------------------------------------------------------------------

## Cleanup

Revert any files created or modified during the test.
