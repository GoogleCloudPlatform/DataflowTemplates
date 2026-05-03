# CDC Data Generator User Guide

The **CDC Data Generator** is a streaming Dataflow template that synthesises
realistic INSERT / UPDATE / DELETE traffic against an existing target schema.
You point it at a Spanner database or a sharded MySQL fleet, tell it how many
rows per second you want, and it discovers the schema, generates well-typed
fake rows that respect primary keys, foreign keys, unique constraints and
parent-child relationships, then writes them to the sink at the requested QPS.

It's primarily used to:

* Drive load tests against migration / replication / CDC pipelines (e.g.
  Spanner change streams, Datastream, reverse-replication).
* Generate baseline data for performance tuning of Spanner and MySQL.
* Produce reproducible CDC traffic for bug repros and integration tests.

This guide covers everything from a 60-second quickstart to the full schema
override DSL, the pipeline architecture, observability, and troubleshooting.

---

## Table of contents

* [Overview](#overview)
  * [How it works](#how-it-works)
  * [Consistency and ordering guarantees](#consistency-and-ordering-guarantees)
* [Before you begin](#before-you-begin)
  * [GCP and Dataflow prerequisites](#gcp-and-dataflow-prerequisites)
  * [Spanner sink prerequisites](#spanner-sink-prerequisites)
  * [MySQL sink prerequisites](#mysql-sink-prerequisites)
  * [Sample Spanner sink-options file](#sample-spanner-sink-options-file)
  * [Sample MySQL source-shards file](#sample-mysql-source-shards-file)
* [Quickstart](#quickstart)
  * [Launch against Spanner](#launch-against-spanner)
  * [Launch against MySQL](#launch-against-mysql)
* [Pipeline parameters reference](#pipeline-parameters-reference)
* [Schema overrides reference](#schema-overrides-reference)
  * [Full HOCON example](#full-hocon-example)
  * [Field reference](#field-reference)
  * [Custom value generators](#custom-value-generators)
* [Architecture](#architecture)
  * [Pipeline DAG](#pipeline-dag)
  * [Stage-by-stage walkthrough](#stage-by-stage-walkthrough)
* [Observe, tune and troubleshoot](#observe-tune-and-troubleshoot)
  * [Pipeline metrics](#pipeline-metrics)
  * [Tuning the QPS knobs](#tuning-the-qps-knobs)
  * [Logs](#logs)
  * [Dead-letter output](#dead-letter-output)
  * [Troubleshooting common errors](#troubleshooting-common-errors)
* [Limitations](#limitations)
* [Contact us](#contact-us)

---

## Overview

### How it works

At launch time the pipeline:

1. Connects to the configured sink (Spanner or MySQL) and reads its DDL,
   building an in-memory `DataGeneratorSchema` that captures every table,
   column type, primary key, foreign key, unique key and (for Spanner)
   interleave relationship.
2. Optionally applies a `--schemaConfig` HOCON / JSON file that lets you
   override per-table QPS, mark columns as skipped, plug in custom Faker
   expressions for column generation, or introduce extra foreign-key
   relationships that aren't expressed in the discovered DDL.
3. Builds a parent → child DAG so child tables are generated together with
   their parents (e.g. one `Order` for each `User`) and so flushes happen in
   FK-safe order.
4. Starts a `PeriodicImpulse` that fires once per second. Each tick is
   multiplied by the schema's total root-table QPS, and each resulting
   sub-tick is mapped to a randomly-selected root table weighted by its
   target QPS.
5. For every selected table, a synthetic primary key is generated and
   reshuffled by `Hash(table#pk) % 5000` so all events for a given row land
   on the same worker.
6. The keyed row is fed to a stateful DoFn (`BatchAndWriteFn`) that completes
   the row, recursively materialises any child rows that should accompany
   this insert, batches them by `(table, shard, op)`, and writes them to the
   sink.
7. Each inserted row optionally schedules N follow-up UPDATEs on a
   processing-time Timer at 5-second intervals (per-table `updateQps`), and
   an optional final DELETE.

### Consistency and ordering guarantees

* **INSERT order**: parent rows always reach the sink before their children.
  Buffer flushes are topologically ordered by the FK / interleave DAG.
* **DELETE order**: children always reach the sink before their parents.
  Buffer flushes happen in *reverse* topological order, and lifecycle DELETEs
  scheduled for the same wall-clock second are sorted deepest-first.
* **UPDATE order**: no ordering guarantee between updates of unrelated rows.
  An UPDATE on a given primary key always lands strictly before that row's
  DELETE.
* **At-least-once**: Beam's standard at-least-once semantics apply. The
  pipeline tolerates worker restarts; the stateful DoFn re-uses the same
  state per (logical) row across bundles.
* **Per-row sticky shard (MySQL)**: a row picks a logical shard at insert
  time and every UPDATE / DELETE for that row routes to the same shard, so
  the row's lifecycle is observable on a single MySQL replica.

---

## Before you begin

### GCP and Dataflow prerequisites

1. A GCP project with the Dataflow, Cloud Storage, and (for Spanner sinks)
   Cloud Spanner APIs enabled.
2. The Dataflow worker service account needs:
   - `roles/dataflow.worker`
   - `roles/storage.objectAdmin` on the staging / temp bucket.
   - `roles/spanner.databaseUser` on the target instance (Spanner sinks only).
   - `roles/secretmanager.secretAccessor` if your MySQL shard file references
     Secret Manager URIs for passwords.
3. Network connectivity from the Dataflow workers to the target sink. For
   MySQL, allowlist Dataflow worker IPs on the database. For Spanner, no
   extra firewall rules are needed when the workers run in the same project.
4. The user launching the template needs `roles/dataflow.admin`,
   `roles/iam.serviceAccountUser` on the worker service account, and access
   to the staging / temp bucket.

If you run the workers without public IPs, pass `--disable-public-ips` and
specify `--subnetwork=https://www.googleapis.com/compute/v1/projects/<proj>/regions/<region>/subnetworks/<subnet>`.

### Spanner sink prerequisites

* The target Spanner database must already exist with the full DDL applied
  *before* you launch the generator. The generator does not create or alter
  tables — it discovers and writes to whatever schema is already there.
* Both **GoogleSQL** and **PostgreSQL** dialects are supported. The dialect
  is detected automatically from the database.
* `INSERT`, `UPDATE`, `DELETE` privileges on every table you want traffic
  for.

### MySQL sink prerequisites

* Each shard you list must already have the target schema applied. The
  generator does not run DDL.
* The MySQL user configured per shard needs `INSERT`, `UPDATE`, `DELETE`,
  and `SELECT` privileges (the last one for schema discovery).
* All shards in your shard file must have **identical schemas**. The
  generator reads the schema from the first shard and assumes the rest match.
* Passwords should live in Google Secret Manager and be referenced via
  `secretManagerUri`. Inline `password` is supported but discouraged for
  production.

### Sample Spanner sink-options file

Save this as `sink_options.json` and upload to GCS — the path goes into
`--sinkOptions`.

```json
{
  "projectId": "my-gcp-project",
  "instanceId": "my-spanner-instance",
  "databaseId": "my-database"
}
```

The dialect (GoogleSQL vs PostgreSQL) is auto-detected; no extra field
needed.

### Sample MySQL source-shards file

Save this as `source-shards.json` and upload to GCS — the path goes into
`--sinkOptions`. Every entry must have a unique `logicalShardId`; the
generator uses these IDs to route lifecycle events back to the same shard
they originated on.

```json
[
  {
    "logicalShardId": "shard1",
    "host": "10.11.12.13",
    "user": "loadgen",
    "secretManagerUri": "projects/123/secrets/loadgen-shard1/versions/latest",
    "port": "3306",
    "dbName": "app_db1"
  },
  {
    "logicalShardId": "shard2",
    "host": "10.11.12.14",
    "user": "loadgen",
    "secretManagerUri": "projects/123/secrets/loadgen-shard2/versions/latest",
    "port": "3306",
    "dbName": "app_db2"
  }
]
```

If you don't (yet) want to use Secret Manager, replace `secretManagerUri`
with an inline `password` field — the generator supports both.

Optional fields: `connectionProperties` (raw JDBC parameters appended after
`?` in the connection URL).

---

## Quickstart

The CDC Data Generator ships as a **flex template** named `Data_Generator`
(container name `data-generator`). The two examples below assume you've
already built and staged the template in your project; for build instructions
see the [Dataflow Templates README](https://github.com/GoogleCloudPlatform/DataflowTemplates).

### Launch against Spanner

```bash
export PROJECT=my-gcp-project
export REGION=us-central1
export TEMPLATE=gs://my-bucket/templates/data-generator.json

gcloud dataflow flex-template run "data-generator-spanner-$(date +%Y%m%d-%H%M%S)" \
  --project="${PROJECT}" \
  --region="${REGION}" \
  --template-file-gcs-location="${TEMPLATE}" \
  --parameters \
sinkType=SPANNER,\
sinkOptions=gs://my-bucket/configs/sink_options.json,\
qpsPerPartition=1000,\
batchSize=100,\
insertQps=5000,\
updateQps=1000,\
deleteQps=500,\
maxShards=1
```

This streams 5 000 inserts / 1 000 updates / 500 deletes per second, batched
in groups of 100, against the Spanner database described in
`sink_options.json`.

### Launch against MySQL

```bash
gcloud dataflow flex-template run "data-generator-mysql-$(date +%Y%m%d-%H%M%S)" \
  --project="${PROJECT}" \
  --region="${REGION}" \
  --template-file-gcs-location="${TEMPLATE}" \
  --parameters \
sinkType=MYSQL,\
sinkOptions=gs://my-bucket/configs/source-shards.json,\
qpsPerPartition=1000,\
batchSize=200,\
insertQps=10000,\
updateQps=2000,\
deleteQps=0,\
maxShards=2,\
schemaConfig=gs://my-bucket/configs/overrides.conf
```

This streams 10 000 inserts / 2 000 updates per second across two MySQL
shards, with a per-table override file applied on top.

---

## Pipeline parameters reference

| Parameter         | Required | Default | Description                                                                                                                                     |
| ----------------- | -------- | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `sinkType`        | Yes      | —       | `SPANNER` or `MYSQL`. Selects the schema fetcher and writer.                                                                                    |
| `sinkOptions`     | Yes      | —       | GCS path to the sink configuration document. Spanner: `sink_options.json`. MySQL: shard list. See samples above.                                |
| `qpsPerPartition` | No       | `1000`  | Target QPS each pipeline partition tries to drive. Used to right-size the tick fan-out for high-throughput runs.                                |
| `batchSize`       | No       | `100`   | Maximum rows buffered per `(table, shard, operation)` before flushing to the sink. Tune up for higher throughput, down for lower memory.        |
| `insertQps`       | No       | `1000`  | Default insert QPS per table. Per-table overrides come from `--schemaConfig`.                                                                   |
| `updateQps`       | No       | `0`     | Default update QPS per table. `0` disables UPDATE generation.                                                                                   |
| `deleteQps`       | No       | `0`     | Default delete QPS per table. `0` disables DELETE generation.                                                                                   |
| `maxShards`       | No       | `1`     | Maximum number of logical shards the generated data is distributed across. For MySQL this is capped by the number of shards in the shard file. |
| `schemaConfig`    | No       | —       | GCS path to a HOCON / JSON file with per-table overrides. See [Schema overrides reference](#schema-overrides-reference).                        |

The pipeline also accepts the standard Beam / Dataflow flags
(`--runner`, `--region`, `--workerMachineType`, `--maxNumWorkers`,
`--disable-public-ips`, `--subnetwork`, `--tempLocation`, `--stagingLocation`,
etc.). For the full list see the [Dataflow pipeline options docs](https://cloud.google.com/dataflow/docs/reference/pipeline-options).

---

## Schema overrides reference

Pass a HOCON or JSON file via `--schemaConfig` to fine-tune what the
generator does for individual tables and columns. HOCON is recommended
because it's tolerant of comments and trailing commas.

### Full HOCON example

```hocon
# overrides.conf — fine-tune per-table and per-column generation
tables {

  # ----- Users: high-volume parent table ----------------------------------
  Users {
    insertQps = 5000
    updateQps = 1000
    deleteQps = 200

    columns {
      # Generate realistic email addresses using a Faker expression.
      email      { generator = "#{internet.emailAddress}" }
      # Use a fixed string for the country column.
      country    { generator = "US" }
      # Don't generate a value — let the sink fill its DEFAULT.
      created_at { skip = true }
    }
  }

  # ----- Orders: child table ----------------------------------------------
  Orders {
    insertQps = 15000   # 3 orders per User on average
    updateQps = 0
    deleteQps = 1500

    # Add a foreign-key relationship that's not declared in the source DDL.
    foreignKeys = [
      {
        name             = "orders_user_fk"
        referencedTable  = "Users"
        keyColumns       = ["user_id"]
        referencedColumns = ["id"]
      }
    ]

    columns {
      sku    { generator = "#{commerce.productName}" }
      amount { generator = "#{number.randomDouble '2','10','5000'}" }
    }
  }
}
```

### Field reference

#### `tables.<name>` (object)

| Field         | Type    | Description                                                                                                                                            |
| ------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `insertQps`   | int     | Target insert QPS for this table. Replaces the global `--insertQps` for this table only.                                                               |
| `updateQps`   | int     | Target update QPS for this table.                                                                                                                      |
| `deleteQps`   | int     | Target delete QPS for this table.                                                                                                                      |
| `columns`     | map     | Per-column overrides. Key is the column name; value is a `ColumnConfig` (see below).                                                                   |
| `foreignKeys` | list    | Additional foreign-key relationships that are not in the discovered DDL. Each entry is a `ForeignKeyConfig` (see below). FK names that match an existing FK must align field-for-field; otherwise the pipeline fails fast at startup. |

#### `tables.<name>.columns.<column>` (object)

| Field       | Type    | Description                                                                                                                                  |
| ----------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `generator` | string  | A Faker expression (e.g. `"#{name.fullName}"`) or a literal value (no `#{...}`). Replaces the default random generator for this column. See [Custom value generators](#custom-value-generators). |
| `skip`      | bool    | If `true`, omit this column from generated INSERT / UPDATE rows so the sink writes its DEFAULT (or NULL). Cannot be set for primary-key columns. |

#### `tables.<name>.foreignKeys[]` (object)

| Field               | Type      | Description                                                          |
| ------------------- | --------- | -------------------------------------------------------------------- |
| `name`              | string    | Unique FK name within the table.                                     |
| `referencedTable`   | string    | The parent table's name.                                             |
| `keyColumns`        | string[]  | Columns on this table participating in the FK (in declaration order). |
| `referencedColumns` | string[]  | Columns on the parent table that `keyColumns` references (same arity, same order). |

### Custom value generators

Two flavours of `generator` value:

* **Faker expressions** start with `#{...}` and are evaluated by
  [DataFaker](https://github.com/datafaker-net/datafaker). Anything the Faker
  library exposes works — `#{name.fullName}`, `#{address.zipCode}`,
  `#{number.randomDouble '2','0','1000'}`, etc. The result is parsed back
  into the column's logical type.
* **Literal values** are taken verbatim. Useful for fixing a column to a
  known constant during a load test (e.g. `country = "US"`).

If parsing the expression result into the column's logical type fails, the
pipeline rejects the configuration at launch time with a descriptive error
naming the column and the failing expression.

---

## Architecture

### Pipeline DAG

```
                    ┌──────────────────────┐
                    │     SchemaLoader     │
                    │ (one-shot side input)│
                    └──────────┬───────────┘
                               │
   ┌───────────────────────────┴──────────────────────────────────┐
   │                                                              │
   │   FetchSchemaFn  ─►  ApplyOverridesFn  ─►  BuildSchemaDagFn  │
   │   (Spanner /          (HOCON merge)         (parent->child   │
   │    MySQL DDL)                                topo + depth)   │
   └──────────────────────────────────────────────────────────────┘
                               │
                               ▼  (DataGeneratorSchema view)
   ┌──────────────────────────────────────────────────────────────┐
   │  PeriodicImpulse(1 s)                                        │
   │           │                                                  │
   │           ▼                                                  │
   │     ScaleTicksFn  (replicate tick × total root-table QPS)    │
   │           │                                                  │
   │           ▼                                                  │
   │     Reshuffle(viaRandomKey)                                  │
   │           │                                                  │
   │           ▼                                                  │
   │     SelectTableFn  (weighted choice over root tables)        │
   │           │                                                  │
   │           ▼                                                  │
   │     GeneratePrimaryKeyFn  (synthesise PK row, with shardId   │
   │           │                if the sink is sharded)           │
   │           ▼                                                  │
   │     MapToReshuffleKey + Reshuffle                            │
   │     (key = "table#" + Hash(table#pk) % 5000  → sticky)       │
   │           │                                                  │
   │           ▼                                                  │
   │     BatchAndWriteFn  (stateful: completes row, recurses to   │
   │           │           children, batches, schedules updates   │
   │           │           and deletes via processing-time timer) │
   │           ▼                                                  │
   │     ┌────────────────┐         ┌───────────────────────┐     │
   │     │ DataWriter     │         │ PCollection<String>   │     │
   │     │ (Spanner |     │         │ FailureRecord JSON    │     │
   │     │  MySQL)        │         │ (DLQ output)          │     │
   │     └────────────────┘         └───────────────────────┘     │
   └──────────────────────────────────────────────────────────────┘
```

### Stage-by-stage walkthrough

**SchemaLoader.** Runs once at pipeline start. `FetchSchemaFn` calls the
sink-specific `SinkSchemaFetcher` (Spanner DDL via the SpannerAdmin client,
MySQL via `INFORMATION_SCHEMA`) and produces a `DataGeneratorSchema`.
`ApplyOverridesFn` merges the optional `--schemaConfig` overrides on top —
per-table QPS values, per-column generator expressions, skip flags, and
extra foreign keys. `BuildSchemaDagFn` walks the FK / interleave graph to
populate each table's `depth` and `childTables` so downstream stages can
process them in topological order. The result is broadcast as a singleton
side input.

**PeriodicImpulse.** A standard Beam source that fires one element per
wall-clock second. This is the metronome that drives generation rate.

**ScaleTicksFn.** Multiplies each tick by `Σ insertQps` over all root
tables, so a single tick produces enough downstream events to satisfy the
configured aggregate QPS. After `Reshuffle.viaRandomKey()` the work fans
out across the worker pool.

**SelectTableFn.** For each scaled tick, picks a root table according to a
weighted distribution where the weight is the table's `insertQps` plus the
recursive QPS of all descendants. Tables higher in the schema DAG get
selected proportionally more often so child tables effectively co-scale.

**GeneratePrimaryKeyFn.** Synthesises a primary-key-only `Row` for the
chosen table. For MySQL it also picks a logical shard id from the shard
file and stamps it onto the row as the synthetic `_dg_shard_id` column so
the row sticks to that shard for its entire lifecycle.

**Reshuffle key.** The PK row is keyed by `Hash(tableName + pk) % 5000` so
two events for the same logical row always land on the same worker —
required because the downstream DoFn is stateful and the state lives per
key. The `% 5000` cap keeps the key cardinality bounded and prevents the
worker pool from fragmenting too thinly.

**BatchAndWriteFn.** Stateful DoFn — the heart of the pipeline. For each
inserted PK row it:

1. Looks up the table in the schema and completes the row by generating
   any missing columns via `DataGeneratorUtils.generateValue`.
2. Stores a *reduced* copy of the row (PK + FK columns + unique columns +
   shard id) in `MapState<activeKeys>` so future UPDATEs can preserve the
   FK / unique values without re-deriving them.
3. Recursively materialises any child rows the schema declares
   (`Order` rows alongside each `User`), computing each child's primary
   key from the parent and pushing them into the same buffer.
4. Schedules N follow-up UPDATEs via a processing-time `Timer`, spaced
   `5 s` apart, where N is `updateQps / insertQps` for the table.
5. Optionally schedules a final DELETE after the last UPDATE.
6. Buffers each row by `(table, shard, op)`. When a buffer hits
   `--batchSize` it flushes — INSERTs in topological order, DELETEs in
   reverse-topological order, UPDATEs unordered.
7. On any sink-write failure, the failed batch is converted to one
   JSON `FailureRecord` per row and emitted to the dead-letter output
   stream. The bundle does **not** crash.

When the row's processing-time timer fires, the DoFn looks up the original
row in state, builds a fresh UPDATE row (PK preserved, FK / unique
preserved, all other columns regenerated) or a DELETE row (PK only, others
nullable null) and buffers it. The DELETE additionally clears the
`activeKeys` entry for that row.

---

## Observe, tune and troubleshoot

### Pipeline metrics

All metrics are surfaced as Beam `Counter`s on the
`com.google.cloud.teleport.v2.templates.dofn.BatchAndWriteFn` namespace and
visible in the Dataflow console under the **Job Metrics** tab.

| Metric                            | Meaning                                                                                         | Use it for                                                          |
| --------------------------------- | ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| `insertsGenerated`                | INSERT rows the DoFn produced (parent + cascaded children).                                     | Confirm tick fan-out is healthy.                                    |
| `updatesGenerated`                | UPDATE rows produced by lifecycle timers.                                                       | Confirm `updateQps` is being honoured.                              |
| `deletesGenerated`                | DELETE rows produced by lifecycle timers.                                                       | Confirm `deleteQps` is being honoured.                              |
| `recordsWritten`                  | Rows successfully written to the sink across all tables / ops.                                  | Headline throughput.                                                |
| `batchesWritten`                  | Successful sink batch calls. Together with `recordsWritten` gives effective batch size.         | Tune `--batchSize`.                                                 |
| `recordsWritten_<shardId>`        | Successful row writes per logical MySQL shard.                                                  | Detect skew across shards.                                          |
| `insert_<table>` / `update_<table>` / `delete_<table>` | Successful row writes per table / op.                                      | Detect tables that are starving or saturating.                      |
| `writeFailures`                   | Rows that hit a sink-write exception and were routed to the DLQ.                                | Should be `0`. Anything else means the sink is rejecting traffic.   |
| `generationFailures`              | Rows that failed during synthesis (before the sink call).                                       | Should be `0`. Indicates a bad `--schemaConfig` generator expression or a model bug. |
| `unresolvableFkChildrenDropped`   | Child rows skipped because the FK couldn't be resolved from the ancestor chain.                 | Should be `0` for a correctly-discovered DDL. Non-zero signals an extra FK in `--schemaConfig` that points at a table not in the parent chain. |
| `tableNotFound_<table>`           | Input rows whose table doesn't exist in the discovered schema.                                  | Should be `0`. Signals stale state vs schema.                       |
| `childTableNotFound_<table>`      | Children referenced by a parent's `childTables()` that aren't in the schema.                    | Should be `0`.                                                      |

### Tuning the QPS knobs

* **`insertQps` / `updateQps` / `deleteQps`** are *targets*. Actual
  throughput is also bounded by the sink (Spanner mutation limit, MySQL
  per-shard write capacity), worker count, and `--qpsPerPartition`.
* If `recordsWritten` lags well below `insertsGenerated`, the sink is the
  bottleneck — increase Spanner nodes / MySQL capacity, or back off the QPS.
* If the worker CPU is pegged but `recordsWritten` is well below the
  sink's headroom, raise `--maxNumWorkers` and `--qpsPerPartition`.
* Increase `--batchSize` to amortise per-batch overhead (especially helpful
  for Spanner where each batch is one mutation transaction). Caveat: too
  large a batch size on MySQL drives up replication lag.
* For a Spanner database with N nodes, a useful starting point is
  `insertQps ≈ 8000 × N`, `batchSize = 100`, `maxNumWorkers = 4 × N`.

### Logs

Worker logs are written to Cloud Logging under the standard Dataflow
labels. Useful filters:

* `severity = WARNING` and `jsonPayload.message =~ "Cannot resolve FK"` —
  catches the unresolvable-FK warnings (also visible via the
  `unresolvableFkChildrenDropped` counter).
* `severity = ERROR` and `jsonPayload.message =~ "Sink write failed"` —
  shows every batch that was routed to the DLQ along with the sink's
  exception.
* `severity = INFO` and `jsonPayload.message =~ "Total QPS resolved to"` —
  fires once at startup, confirms the schema's aggregate root QPS.

### Dead-letter output

`BatchAndWrite` emits a `PCollection<String>` of newline-delimited JSON
failure records — one per row that the sink rejected or whose generation
threw. Each record has the shape:

```json
{
  "timestamp": "2026-05-03T12:34:56.789Z",
  "table":     "Orders",
  "operation": "INSERT",
  "row": { "id": 42, "user_id": 17, "amount": 19.99, "_dg_shard_id": "shard1" },
  "error": {
    "class":      "com.google.cloud.spanner.SpannerException",
    "message":    "FAILED_PRECONDITION: Row [42] in table Orders is referenced by a foreign key from table OrderItems.",
    "stackTrace": "..."
  }
}
```

The pipeline does not write this stream anywhere by default — chain a
`TextIO.write()` to it (or pipe to PubSub / BigQuery) if you want to
persist failures for analysis.

### Troubleshooting common errors

| Symptom                                                                                          | Likely cause                                                                                          | Fix                                                                                                                                                              |
| ------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Pipeline starts but no rows reach the sink (`insertsGenerated = 0`)                              | Aggregate root-table QPS is `0`. Either every table has `insertQps = 0` or no tables are roots.       | Check `--insertQps` and per-table overrides. The `Total QPS resolved to: 0` log line fires when this happens.                                                    |
| `IllegalStateException: Total QPS must be greater than zero`                                     | Same as above, but caught early.                                                                      | Provide a non-zero `insertQps` somewhere in the schema.                                                                                                          |
| `unresolvableFkChildrenDropped` is non-zero                                                      | A FK declared in `--schemaConfig` points at a table that is not an ancestor of the child.             | Either remove the FK from the override, or restructure the schema so the referenced table actually parents the child.                                            |
| `writeFailures` is non-zero                                                                      | The sink rejected a batch. Inspect the DLQ output for the exception class.                            | Spanner `FAILED_PRECONDITION`: usually a constraint violation; tighten `--schemaConfig` generators. MySQL `Duplicate entry`: lower `insertQps` for that table.   |
| `generationFailures` is non-zero                                                                 | A `--schemaConfig` generator expression failed to evaluate or the produced value can't be parsed into the column type. | Inspect the DLQ record's `error.message` — it names the column and expression. Fix the expression or relax the column type.                                      |
| `Foreign key '<name>' on table '<t>' conflicts with the discovered definition`                   | An override-FK has the same name as a discovered FK but different columns / referenced table.         | Rename the override-FK or align it field-for-field with the discovered one.                                                                                      |
| `Cannot skip primary-key column '<c>' in table '<t>'`                                            | `--schemaConfig` set `skip = true` on a PK column. PKs are required for state-keying.                 | Remove the `skip` flag for the PK column; you can still skip non-PK columns.                                                                                     |
| `Failed to read shards from <gcs path>` (MySQL)                                                  | Shard file is missing, malformed JSON, or no entries.                                                 | Check the file is uploaded, is valid JSON, and contains at least one shard entry with all required fields.                                                       |
| `No tables available for selection (total weight may be 0)`                                      | The schema has no tables marked `isRoot = true`.                                                      | This is unusual — `BuildSchemaDagFn` flags as roots any table that is not the child of another table. Check that your DDL doesn't make every table a child somehow. |
| Spanner job stalls, Dataflow throughput drops to zero, no obvious error                          | Spanner mutation limit exceeded, autoscaling backed off.                                              | Lower `--insertQps` and / or `--batchSize`. Add Spanner nodes.                                                                                                    |
| MySQL job has very uneven throughput across shards                                               | Schema-driven skew (one table dominates, all its FK targets sit on one shard).                        | Distribute parent rows across more shards (raise `--maxShards`); or reduce that table's `insertQps`.                                                              |

---

## Limitations

* **Logical types**: `STRING`, `INT64`, `FLOAT64`, `NUMERIC`, `BOOLEAN`,
  `BYTES`, `DATE`, `TIMESTAMP`, `JSON`, `UUID`, and `ENUM` are fully
  supported. `ARRAY<...>` and `STRUCT` are partially supported (Spanner
  only, with element-type generation) — exotic nested types may fall back
  to a placeholder `"unknown"` string.
* **Schema discovery sinks**: Spanner and MySQL only. Other sinks (e.g.
  PostgreSQL, BigQuery) are not supported.
* **DDL changes during a running job**: not supported. The schema is read
  once at pipeline start and used for the lifetime of the job. Restart the
  job after a DDL change.
* **Per-row UPDATE cap**: when an ancestor row has a scheduled DELETE, the
  number of UPDATEs the generator will produce for that row's children is
  capped so the last UPDATE strictly precedes the ancestor's DELETE. Heavy
  DELETE workloads with very high `updateQps` may therefore see fewer
  UPDATEs than the configured target.
* **Cross-table foreign keys with multiple references to the same parent**
  (e.g. `manager_id` and `assistant_id` both pointing at `Employee`) are
  not currently supported and are rejected at schema-load time. Use a
  single FK or split the table.
* **Dialect detection**: Spanner GoogleSQL vs PostgreSQL is auto-detected.
  Mixed-dialect databases are not supported.
* **No schema bootstrap**: the generator never creates or alters tables.
  Apply your DDL before launching.

---

## Contact us

For bugs, feature requests, or operational questions please file an issue
in the [DataflowTemplates repository](https://github.com/GoogleCloudPlatform/DataflowTemplates/issues),
or reach out to the team on the channel listed in your project's
`CONTRIBUTING.md`.
