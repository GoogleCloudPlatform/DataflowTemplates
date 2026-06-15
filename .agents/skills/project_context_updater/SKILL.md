---
name: project-context-updater
description: >-
  Reads a project-context.md file to quickly get context on a project's
  architecture and tools. Update the project-context.md
  file AFTER completing a code change to ensure the documentation remains
  accurate and to add any new learnings, troubleshooting tips, or example
  PRs to the AI Agent Tips section. Use this skill to quickly ramp up on a
  codebase using project-context.md. Don't use when instructed to create a
  project-context.md file from scratch (use
  project-context-generator instead).
---

# Project Context Updater

This skill encourages agents to consume `project-context.md` for rapid
onboarding, and guides them on how to update the file subsequently after
navigating code and executing tests.

## 1. Initial Ramp-Up

When beginning a task in a project directory that contains a
`project-context.md` file:

1.  Read the file to familiarize yourself with the project's goal, primary
    users, and technical details.
2.  Pay special attention to the `AI Agent Tips` and `Build/Run Commands`
    sections.
3.  Store this information for use while developing your changes or navigating
    the project.

## 2. Post-Code Change Update Checklist

**AFTER** you have successfully made changes to the repository and are preparing
your code for review, run through this checklist to see if the
`project-context.md` needs updating.

Copy this checklist and track your progress:

-   [ ] Step 1: Validate Build Commands
-   [ ] Step 2: Architecture Diagram Updates
-   [ ] Step 3: Validate Directory Structure Mapping
-   [ ] Step 4: Validate Tech Stack Versions
-   [ ] Step 5: Update Coding Standards & Testing Frameworks
-   [ ] Step 6: AI Agent Tips Updates
-   [ ] Step 7: Prune Outdated Context

### Step 1: Validate Build Commands

Does the `README.md` still accurately describe how to build and test the software? If build processes have changed, ensure the README is updated, and the `project-context.md` still properly points to it.

### Step 2: Architecture Diagram Updates

Did your changes affect the core logical flow of the project? Review the
architecture diagram / SVG graph. If it no longer reflects reality due to your
changes across the stack, update the DOT file, generate a new SVG, and embed the
updated diagram. Always keep the `.dot` and `.svg` files in sync.

### Step 3: Validate Directory Structure Mapping

Did your changes move, rename, or introduce new packages? Review the `Project Structure (Logical Architecture Mapping)` section. If the mapping between logical stages and exact directory paths is outdated, update it so AI agents maintain accurate spatial awareness.

### Step 4: Validate Tech Stack Versions

Did you upgrade a dependency, change a framework version, or encounter a new limitation? Update the `Tech Stack & Versions` section to ensure exact versions are pinned and limitations are documented to prevent future AI hallucination.

### Step 5: Update Coding Standards & Testing Frameworks

Did you introduce a new design pattern, testing framework (e.g., migrating from JUnit 4 to JUnit 5), or establish a new rule? Update the `Coding Standards & Best Practices` and `Testing Frameworks & Guidelines` sections to provide clear rules to prevent the AI from generating incompatible or verbose code in the future.

### Step 6: AI Agent Tips Updates

Did you encounter any particular challenges while working on your changes? If
you found any subtle project quirks, new architectural decisions, or tricky API interactions,
document these in the respective `AI Agent Tips` subsections:
*   **Core Architectural Decisions:** Log finalized "Why we did X instead of Y" decisions.
*   **Known Issues & Quirks:** Persistent bugs, workarounds, or limitations.
*   **Lessons Learned & Ah-ha Moments:** Non-obvious solutions or debugging breakthroughs.

*   **Example PRs**: Add a placeholder for your PR (or another representative PR) to the Example
    PRs section, or update it with the actual PR link after creation to help guide future AI agents.

### Step 7: Prune Outdated Context

Review the `AI Agent Tips`. If any entries are no longer relevant due to your recent changes, or if the section has become overly verbose, condense or delete them. Actively manage the file to prevent context window bloat.

## 3. Best Practices for Humans

To keep the `project-context.md` valuable, interact with the agent frequently to explicitly save context:

*   **The Session Starter:** Kick off new chats with: *"Read project-context.md, then let's work on..."*
*   **The Explicit Save:** If you spend time debugging a complex issue, explicitly prompt the agent: *"Summarize the fix we just found and update the Lessons Learned section in project-context.md so we don't forget it."*
*   **Keep it Pruned:** If the file is getting large, ask the agent: *"Review project-context.md and condense any outdated or overly verbose points."*

## 4. Creating the PR

Once updates are made, commit the project-context.md changes. If updating an existing PR, push the changes to the remote branch. If creating a new PR, use gh pr create.
If needed, ask the user to authenticate to git/GitHub (e.g., `gh auth login`).

## Reporting Issues

Report bugs or improvements for this skill in the repository's issue tracker.
