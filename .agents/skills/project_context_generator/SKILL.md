---
name: project-context-generator
description: >-
  Generates a comprehensive project-context.md documentation file for a
  project. It discovers code directories, build commands, and architecture.
  Use when asked to document a project's context. Don't use when instructed to review or
  update an existing project-context.md.
---

# Project Context Generator

This skill provides a standardized set of instructions to generate a
comprehensive `project-context.md` file. This file helps new developers and AI
agents quickly understand a project's architecture, tech stack, and essential
workflows.

## Prerequisites

Before starting, attempt to automatically detect the local git repository root (e.g., using git rev-parse --show-toplevel). If automatic detection fails, ask the user for:

*   The absolute path to the local git repository root.
*   If needed, ask the user to authenticate to git/GitHub (e.g., `gh auth login`).
*   Links to important documentation for the project.

## 1. Deep-Dive Research & Confirmation

Perform deep-dive research to discover the information needed to fill out the
template.

Copy this checklist and track your progress:

-   [ ] Step 1: Analyze Documentation and Code Context
-   [ ] Step 2: Verify Findings Through Code
-   [ ] Step 3: Figure out Architecture Diagram and Dependency Tree
-   [ ] Step 4: Formulate Project Structure Mapping
-   [ ] Step 5: Determine Build/Test Commands & Tech Stack Versions
-   [ ] Step 6: Define Testing Frameworks
-   [ ] Step 7: Find Example PRs

### Step 1: Analyze Documentation and Code Context

Ask the user for links to important documentation and go through them along with the earlier context of the code to figure out:
*   Core Intent
*   Data flow
*   Important terminology
*   Coding standards, best practices, and Gotchas. Ensure these are facts important from a development and coding perspective (e.g., testing practices, architectural caveats) and NOT from a user/operator perspective.

### Step 2: Verify Findings Through Code

Verify the findings from Step 1 (Core Intent, Data flow, terminology, standards, gotchas) directly through the code files. Documentation might be stale, so the code must act as the ultimate source of truth.

### Step 3: Figure out Architecture Diagram and Dependency Tree

Go through the code files to figure out the architecture and dependency tree. Write a GraphViz (`.dot`) file representing the architecture and dependency tree, and compile it to an SVG diagram. Ensure you explicitly instruct the agent to always keep the `.dot` and `.svg` files in sync.

### Step 4: Formulate Project Structure Mapping

Formulate the project structure mapping to help any AI agent working on that template to have a general idea of the code and different important aspects of the code from the get-go. Analyze the repository to map pipeline stages or logical components directly to exact package/directory paths. **Ensure ALL folders and subdirectories in the codebase are fully mapped and accounted for** to give agents complete spatial awareness.

### Step 5: Determine Build/Test Commands & Tech Stack Versions

*   Look for build files (like `pom.xml`, `build.gradle`, `Makefile`, etc.) and verify commands by running them (e.g., `mvn clean test`, `npm run build`). Direct the user to the `README.md` file in the `project-context.md` instead of hardcoding the commands to avoid duplication.
*   Identify the exact versions of the tech stack (e.g., Java 17, Beam 2.XX, specific database driver versions) and note any limitations to prevent hallucinating newer features or deprecated APIs.

### Step 6: Define Testing Frameworks

Check the pre-existing testing patterns and practices. Detail the exact testing stack (e.g., JUnit 4 vs 5, Mockito, Truth) and provide rules/best practices to prevent the AI from guessing between different libraries and generating incompatible code.

### Step 7: Find Example PRs

Search recently merged PRs or recent commits specifically touching the project directory (e.g., git log -n 20 -- <project-directory>) to find good representative examples of how a new feature request or bug-fix looks like for that template. Avoid using simple or test-only PRs.

### Confirmation

**Before generating the final document**, report back your findings to the user
for confirmation. Wait for their approval.

## 2. Document Generation

After the user approves your research findings:

1.  Create a new git branch for the project using `git checkout -b <branch-name>`.
2.  Synthesize all gathered information into a concise Markdown file following
    the structure of the template provided in
    [project-context.template.md](references/project-context.template.md).
    *   Focus on "Need to Know" information for a developer making their first
        PR.
    *   If information is missing, leave the section as `[TBD]`.
    *   Use relative repository paths for all code references to ensure portability across different environments.3.  Write the generated Markdown content to a file named `project-context.md` in
    the root directory of the project.
4.  Embed the SVG architecture diagram in the document.
5.  Commit your changes and upload a PR for review (`git commit`, `gh pr create`).

## Reference Template

The template structure can be found in
[project-context.template.md](references/project-context.template.md).