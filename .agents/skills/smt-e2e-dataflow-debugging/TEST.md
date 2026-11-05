# Tests for smt-e2e-dataflow-debugging skill

Verify the skill triggers and executes correctly for debugging logical errors in Dataflow jobs.

## Case 1: Debugging Schema Mapping Issues

**Prompt:**
`/smt-e2e-dataflow-debugging test_job.tfvars`

**Expected Outcome:**
- The agent should initialize Terraform and launch the Dataflow job.
- The agent should monitor the job status and retrieve logs upon completion.
- The agent should inspect and compare the data between the Cloud SQL source and the Spanner destination.
- The agent should identify any schema mapping discrepancies, locate the offending code, propose a fix, rebuild the template, and clean/re-run the test to verify the fix.
