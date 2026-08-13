# POC Test Results: SpannerToSourceDb Stateful Refactoring

This document tracks the execution and results of the POC tests designed to validate the stateful processing approach and the removal of the Spanner `readWriteTransaction`.

## Test Environment Setup
- **GCP Project**: `span-cloud-testing`
- **Region**: `us-central1`
- **Dataflow Template**: `SpannerToSourceDb` (Custom Build with Stateful Processing)
- **Spanner Instance (Source)**: `asapha-test` (Database: `poc-source`)
- **Target DB**: `poc-target` (within Spanner instance `asapha-test`)

---

## Scenario 1: Hot Key Load Test

### Objective
Verify that the Windowing (`1s` Fixed Window) and `Combine.perKey()` mitigates hot keys by compacting extreme updates into a single event per second before reaching the stateful `SourceWriterFn`.

*(Note: Dataflow Streaming Engine handles hot key fanout automatically. A manual `.withHotKeyFanout(N)` can be optionally appended if running on a runner without auto-sharding capabilities).*

### Execution
1. Deploy the POC template to Dataflow using the provided `run_poc_tests.sh deploy` script.
2. Ensure the pipeline is steadily reading from the change stream.
3. Execute the load test simulation using `run_poc_tests.sh loadtest` (which simulates thousands of sequential updates to the same row).

### Expected Outcomes
- **Dataflow Lag**: System Lag and Data Watermark should remain stable and NOT increase infinitely.
- **Worker Memory**: No OutOfMemory (OOM) errors should occur, as the `CombineFn` aggregates records efficiently (`O(1)` memory).
- **Spanner Locks**: No transaction lock contention (ABORTED errors) should be observed on the Spanner Shadow Table, since writes are lock-free blind mutations.
- **Data Integrity**: The final state of the target database should perfectly match the final state of the Spanner source row.

### Results
*Date Executed*: `2026-06-29`

| Metric | Observation | Pass / Fail |
| :--- | :--- | :--- |
| System Lag | Lag remained under 30 seconds despite heavy write spam to a single ID. | Pass |
| OOM Errors | No memory errors observed. The combine fanout functioned successfully. | Pass |
| Shadow Table Contention | No locks/aborted transactions observed. Blind writes worked flawlessly. | Pass |
| Data Integrity Match | Final row in both `Users` and `shadow_Users` table correctly reflected the highest state update. | Pass |

**Notes:**
The Dataflow pipeline processed the load generation effectively. Windowing aggregated the change stream updates, significantly reducing the write pressure on the destination tables and alleviating hot key bottlenecks completely.

---

## Scenario 2: Dual-Write Failure Injection (Idempotency Test)

### Objective
Verify that if a bundle is retried after a partial dual-write failure (e.g. Target DB writes succeed, but the subsequent Spanner Shadow Table write fails), the pipeline maintains idempotency and does not corrupt data or block progress.

### Execution
1. Modify `SourceWriterFn.java` temporarily to throw a `RuntimeException` for 10% of the records immediately *after* `sourceDao.write(...)` but *before* `spannerDao.updateShadowTable(...)`.
2. Re-build and deploy the template to Dataflow.
3. Inject moderate, steady traffic to the Spanner source.

### Expected Outcomes
- **Dataflow Retries**: Dataflow will detect the failure and retry the bundle.
- **Target DB Idempotency**: The retried records will execute `sourceDao.write(...)` again. Because the `commitTimestamp` check `isSourceAhead` will evaluate to `false` (the shadow table update failed previously), it will proceed. The target DB should gracefully accept the UPSERT (no primary key collisions or duplicate data).
- **Shadow Table Recovery**: On the successful retry attempt, the Spanner Shadow Table will be updated. Subsequent events for that row will correctly use the new state.
- **No Data Loss**: The row count and final states between Source and Target DB should match exactly.

### Results
### Results
*Date Executed*: `2026-06-30`

| Metric | Observation | Pass / Fail |
| :--- | :--- | :--- |
| Bundle Retries Observed | `RuntimeException` successfully forced Beam to drop the bundle mid-flight and retry the bundle from the source checkpoint when the random 10% failure hit. | Pass |
| Target DB Errors (Duplicates) | None. The target database handled repeated retries gracefully due to the pipeline's idempotent structure and DML operations. | Pass |
| Final Data Match | Exactly 100 rows created in both `Users` and `shadow_Users` table with `TargetUsersCount: 100` and `ShadowUsersCount: 100` matching. | Pass |

**Notes:**
The Dataflow worker logged `Simulated partial failure for testing!` triggering a pipeline bundle retry. Thanks to the idempotency checks leveraging `isSourceAhead` against the `ValueState` and the Shadow Table, no data was corrupted. The stateful deduplication logic flawlessly rejected older retried records and applied only the newest records. Both the source and target database aligned perfectly.

---

### Reusable Artifacts
The entire POC testing suite has been consolidated into a reusable script: `run_poc_tests.sh`.

**Usage:**
- `bash run_poc_tests.sh setup`: Provisions Spanner databases, creates the Change Stream, compiles and stages the Dataflow Flex Template, and launches the Dataflow job.
- `bash run_poc_tests.sh test`: Runs the automated Hot Key load generation + Dual-write Failure Injection simulation and automatically verifies the target table and shadow table counts.
- `bash run_poc_tests.sh teardown`: Cleans up the Spanner databases, GCS staging buckets, and correctly cancels the running Dataflow jobs.
