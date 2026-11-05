# Workspace Skills Index
Auto-generated index of available skills.

## smt-e2e-dataflow-debugging
**Directory**: `.agents/skills/smt-e2e-dataflow-debugging`

- Debugs logical errors and data discrepancies in Dataflow templates by launching jobs via Terraform and comparing source (e.g. Cloud SQL) vs destination (e.g. Spanner) data. Use ONLY when working with one of these specific migration templates: gcs-spanner-dv, sourcedb-to-spanner, datastream-to-spanner, spanner-to-sourcedb.

## smt-functional-testing
**Directory**: `.agents/skills/smt_functional_testing`

- Functionally tests local Dataflow pipeline changes against the main branch using ephemeral GCP resources and gated approvals. Use ONLY when functionally testing one of these specific migration templates: gcs-spanner-dv, sourcedb-to-spanner, datastream-to-spanner, spanner-to-sourcedb. Skip entirely for other templates. Don't use for deploying templates to production or debugging a running production pipeline without testing.