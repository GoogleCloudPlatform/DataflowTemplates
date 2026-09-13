# Workspace Skills Index
Auto-generated index of available skills.

## smt-e2e-dataflow-debugging
**Directory**: `.agents/skills/smt-e2e-dataflow-debugging`

- Debugs logical errors and data discrepancies in Dataflow templates by launching jobs via Terraform and comparing source (e.g. Cloud SQL) vs destination (e.g. Spanner) data. Use ONLY when working with one of these specific migration templates: gcs-spanner-dv, sourcedb-to-spanner, datastream-to-spanner, spanner-to-sourcedb.

## smt-functional-testing
**Directory**: `.agents/skills/smt_functional_testing`

- Functionally tests local Dataflow pipeline changes against the main branch using ephemeral GCP resources and gated approvals. Use ONLY when functionally testing one of these specific migration templates: gcs-spanner-dv, sourcedb-to-spanner, datastream-to-spanner, spanner-to-sourcedb. Skip entirely for other templates. Don't use for deploying templates to production or debugging a running production pipeline without testing.

## project-context-generator
**Directory**: `.agents/skills/project_context_generator`

- Generates a comprehensive project-context.md documentation file for a project. It discovers code directories, build commands, and architecture. Use when asked to document a project's context. Don't use when instructed to review or update an existing project-context.md.

## project-context-updater
**Directory**: `.agents/skills/project_context_updater`

- Reads a project-context.md file to quickly get context on a project's architecture and tools. Update the project-context.md file AFTER completing a code change to ensure the documentation remains accurate and to add any new learnings, troubleshooting tips, or example PRs to the AI Agent Tips section. Use this skill to quickly ramp up on a codebase using project-context.md. Don't use when instructed to create a project-context.md file from scratch (use project-context-generator instead).