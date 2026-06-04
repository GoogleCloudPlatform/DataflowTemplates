# Workspace Skills Index
Auto-generated index of available skills.

## migrations-functional-testing
**Directory**: `.agent/skills/migrations_functional_testing`

-
  Functionally tests local Dataflow pipeline changes against the main branch using ephemeral GCP resources and gated approvals.
  Use ONLY when functionally testing one of these specific migration templates: gcs-spanner-dv, sourcedb-to-spanner, datastream-to-spanner, spanner-to-sourcedb.
  Skip entirely for other templates. Don't use for deploying templates to production or debugging a running production pipeline without testing.