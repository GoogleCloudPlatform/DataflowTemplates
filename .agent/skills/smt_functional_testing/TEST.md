# Tests for smt-functional-testing skill

Verify the skill triggers and executes correctly for typical functional testing tasks.

## Case 1: Custom Transformation Pipeline with Edge Cases

**Prompt:**
`/smt-functional-testing What to test: I want to test Custom transformations in the template @[v2/sourcedb-to-spanner].`

**Expected Outcome:**
- The agent should check the git diff and acknowledge no new changes were made. Proceed with testing the template with edge cases as requested.
- The agent should generate test scenarios covering custom transformations, row filtering, primary key transformations, and column type conversions, stopping at Approval Gate 1.
- After approval, the agent should prepare the schema (DDL/DML) and configuration files (`.env.testing`, custom transformation JAR, `dataflow_job.tf`, `tfvars`), prompting for approvals.
- The agent should provision resources using `terraform apply` for the template with the requested parameters and monitor the Dataflow job.
- The agent should verify that valid records were inserted into the target Spanner database and that exceptions/filtered rows were correctly routed to the Cloud Storage `dlq/` and `filteredEvents/` directories.
- The agent should successfully handle requests to re-perform the migration with additional parameters (like `gcsOutputDirectory`) and verify the output (e.g. AVRO generation).
- Finally, the agent should update the functional test report, and draft teardown commands (`terraform destroy`), stopping at the final Approval Gate.
