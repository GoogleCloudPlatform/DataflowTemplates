# Project Context: [Project Name]

<!-- 
AI SYSTEM DIRECTIVES (CRITICAL):
1. Role: Act as a Senior Software Engineer for this project.
2. Read: You must parse this document before starting any task.
3. Write/Override: You are explicitly authorized to overwrite, modify, or delete existing entries in this file when they become outdated or are superseded by new decisions.
4. Maintain: Actively prune the "AI Agent Tips" section to prevent context window bloat.
5. Plan: When creating or modifying an Implementation Plan artifact, you MUST explicitly include a step to review this project-context.md file to ensure your plan aligns with established architectural decisions and gotchas.
-->

## Overview

*   **Core Intent:** (High-level summary of what this project solves).
*   **Primary Users:** (Who uses this? e.g., "SREs," "End-users via Frontend X").
*   **Terminology:** (Define project-specific acronyms).

## Technical Details

*   **Tech Stack & Versions:**
    <!-- AI Agent: Identify and pin the exact versions of the tech stack (e.g., Java 11, Beam 2.XX) and note any limitations to prevent hallucinating newer features or deprecated APIs. -->
    *   **Languages:** (e.g., Python 3.9, Java 11, Go 1.20)
    *   **Frameworks/Libraries:** (e.g., Angular 15, Apache Beam 2.48.0)
    *   **Key Technologies:** (e.g., Spanner, Kafka, Redis)
*   **Code Location:** [Link to Repository root]
*   **Data Flow:** (Briefly describe how data enters and exits).
*   **Project Structure (Logical Architecture Mapping):**
    <!-- AI Agent: Formulate project structure mapping to help any AI agent working on that template to have a general idea of the code and different important aspects of the code from the get-go. Ensure ALL folders and subdirectories in the codebase are fully mapped and accounted for. -->
    *   `(exact/path/to/source_readers)`: (e.g., Source Readers)
    *   `(exact/path/to/transformers)`: (e.g., Transformers)
    *   `(exact/path/to/writers)`: (e.g., Spanner Writers)
*   **Build/Run Commands:**
    <!-- AI Agent: Direct developers and agents to the project's README.md for up-to-date execution instructions to avoid duplication. -->
    See the `README.md` file for instructions on building and running the pipeline.

## Documentation

*   **Architecture Diagram & Dependency Tree:** [architecture.svg](architecture.svg) (Source: `architecture.dot`).
    <!-- AI Agent: Deep dive into the code to understand the architecture and dependency tree. Generate a GraphViz DOT file, convert it to SVG, and embed the SVG into this document. Add the DOT and SVG files to the repository. -->
    *   **Rule:** Always keep the `.dot` and `.svg` files in sync. If you modify the architecture, you MUST regenerate the `.svg` from the `.dot` file.

## AI Agent Tips

*   **Common Tasks:** (Examples of tasks an AI might help with, e.g., adding new
    API endpoints, writing unit tests, refactoring modules)
*   **Coding Standards & Best Practices:**
    <!-- AI Agent: Check pre-existing code patterns and provide a list of rules/best practices for the AI to follow (e.g., "Use AutoValue for POJOs", "Avoid raw functional programming steps"). -->
    *   (Rule 1)
    *   (Rule 2)
*   **Testing Frameworks & Guidelines:**
    <!-- AI Agent: Check pre-existing testing patterns and specify the exact testing stack (e.g., JUnit 4, Mockito, Truth) and rules to prevent incompatible code generation. -->
    *   **Frameworks:** (e.g., JUnit 4, Truth for assertions)
    *   **Rules:** (e.g., "Use @RunWith(JUnit4.class)", "Mock Spanner with X")
*   **Core Architectural Decisions:** (Log finalized technical decisions, e.g., "Why we did X instead of Y")
*   **Known Issues & Quirks:** (Persistent bugs, workarounds, or limitations. e.g., critical business logic, legacy sections, code with high impact)
*   **Lessons Learned & Ah-ha Moments:** (Non-obvious solutions or debugging breakthroughs discovered during development)
*   **Example PRs:**
    <!-- AI Agent: Find recent representative PRs (via git log or gh pr list) that demonstrate how a new feature request or bug-fix looks like. Avoid simple or test-only PRs. -->
    *   [PR 1](Link to PR) - (Adding a new feature)
    *   [PR 2](Link to PR) - (Bug fix)
