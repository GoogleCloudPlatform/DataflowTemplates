
BigQuery Anomaly Detection (Experimental)
---
> **Note:** This template is experimental and may change without notice.

A streaming Dataflow Flex Template that monitors a BigQuery table for anomalies
in real time. The pipeline reads CDC (Change Data Capture) data from BigQuery,
computes a configurable windowed metric, runs statistical anomaly detection, and
publishes detected anomalies to a Pub/Sub topic.

Supported anomaly detectors: **ZScore**, **IQR**, **RobustZScore** (from
Apache Beam's `apache_beam.ml.anomaly` module), and **Threshold** (a simple
fixed-threshold alerter based on a boolean expression).

## Parameters

### Required parameters

* **table**: BigQuery table to monitor. Format: `project:dataset.table`.
* **metric_spec**: JSON string defining the metric computation (see [Metric Spec Reference](#metric-spec-reference) below).
* **detector_spec**: JSON string defining the anomaly detector (see [Detector Spec Reference](#detector-spec-reference) below).
* **topic**: Pub/Sub topic for anomaly results. Full path: `projects/<project>/topics/<topic>`.

### Optional parameters
* **poll_interval_sec**: Seconds between BigQuery CDC polls. Default: `60`.
* **change_function**: BigQuery change function: `APPENDS` or `CHANGES`. Default: `APPENDS`.
* **buffer_sec**: Safety buffer behind `now()` in seconds. Default: `15`.
* **start_offset_sec**: Start reading from this many seconds ago. Default: `60`.
* **duration_sec**: How long to run in seconds. `0` means run forever. Default: `0`.
* **temp_dataset**: BigQuery dataset for temp tables. If unset, auto-created.
* **log_all_results**: Log all anomaly detection results (normal, outlier, warmup) at WARNING level. Default: `false`.
* **sink_table**: BigQuery table to write all anomaly detection results to. Format: `project:dataset.table`. If unset, results are not written to BigQuery.
* **decompress_shards**: Number of shards for CDC Arrow batch decompression fan-out. Spreads decompression CPU across workers. `0` disables fan-out (decode inline). Default: `400`.
* **fanout_strategy**: Parallelism strategy for metric aggregation: `sharded`, `hotkey_fanout`, `precombine`, or `none`. Default: `sharded`. See [Fanout Strategies](#fanout-strategies).
* **fanout**: Number of shards for `sharded` or `hotkey_fanout` strategies. Ignored for `none` and `precombine`. Default: `400`.

## Metric Spec Reference

The `metric_spec` parameter is a JSON string that defines how raw rows are
aggregated into a single numeric value for anomaly detection.

```json
{
  "aggregation": {
    "window": {
      "type": "fixed",
      "size_seconds": 3600
    },
    "group_by": ["field1", "field2"],
    "measures": [
      {"field": "amount", "agg": "SUM", "alias": "total"}
    ]
  },
  "derived_fields": [
    {"name": "is_success", "expression": "1 if status == 'success' else 0"}
  ],
  "measure_combiner": {"expression": "clicks / impressions"}
}
```

| Field | Required | Description |
|---|---|---|
| `aggregation` | Yes | Windowed aggregation configuration. |
| `aggregation.window.type` | Yes | `fixed` or `sliding`. |
| `aggregation.window.size_seconds` | Yes | Window size in seconds. |
| `aggregation.window.period_seconds` | Sliding only | Slide period in seconds. |
| `aggregation.group_by` | No | Field names for grouping. Omit for global aggregation. |
| `aggregation.measures` | Yes | List of aggregation measures. |
| `aggregation.measures[].field` | Yes | Input field name (ignored for `COUNT`). |
| `aggregation.measures[].agg` | Yes | `SUM`, `COUNT`, `MIN`, `MAX`, or `MEAN`. |
| `aggregation.measures[].alias` | Yes | Output name for this measure. |
| `derived_fields` | No | Pre-aggregation computed columns. |
| `measure_combiner` | When >1 measure | Post-aggregation expression combining measure aliases. |

Expressions support: `+`, `-`, `*`, `/`, `//`, `%`, `**`, comparisons,
`and/or/not`, `if/else`, safe builtins (`abs`, `min`, `max`, `round`),
and parentheses. Bare names are field references.

## Detector Spec Reference

```json
{"type": "ZScore"}
{"type": "ZScore", "config": {"window_size": 500}}
{"type": "ZScore", "config": {"threshold_criterion": {"type": "FixedThreshold", "config": {"cutoff": 10}}}}
```

| Detector | Description | Default threshold |
|---|---|---|
| `ZScore` | `\|value - mean\| / stdev` | 3 |
| `IQR` | Interquartile Range | 1.5 |
| `RobustZScore` | Modified Z-Score using median/MAD | 3.5 |
| `Threshold` | Fixed threshold alert via boolean expression | N/A |

**Threshold** evaluates a boolean expression against the metric `value` and
fires an alert (label=1) when the expression is true:

```json
{"type": "Threshold", "expression": "value >= 100"}
{"type": "Threshold", "expression": "value > 100 or value < -100"}
{"type": "Threshold", "expression": "value <= 0.01"}
```

The `window_size` shorthand (default: 1000) sets the history buffer for all
internal statistical trackers.

### Threshold overrides

```json
{"type": "FixedThreshold", "config": {"cutoff": 10}}
{"type": "QuantileThreshold", "config": {"quantile": 0.95}}
```

## Pub/Sub Output

Detected anomalies (label == 1) are published to the configured Pub/Sub topic
as JSON messages:

```json
{
  "event_description": "Anomaly detected value=1234.56 score=4.2 in window=2026-03-19T12:00:00.000000Z-2026-03-19T13:00:00.000000Z",
  "agent_id": "ZScore",
  "key": "(campaign_a, chrome)"
}
```

The `key` field is only present for grouped (keyed) metrics.

Set `--log_all_results` to log all results (normal, outlier, warmup) at
WARNING level in the Dataflow worker logs.

## Fanout Strategies

The `fanout_strategy` pipeline parameter controls how elements are distributed
across intermediate reducers before the final merge. Applies to both keyed and
global aggregation.

| Strategy | How it works | Best for |
|---|---|---|
| `sharded` (default) | Per-element random sharding into N shard keys. Stage 1 `CombinePerKey` reduces each shard independently. Stage 2 merges N partial accumulators. | High-throughput aggregation (e.g., 1M+ rows/sec). Uniform distribution regardless of bundle count. |
| `hotkey_fanout` | Beam's built-in `Combine.with_fanout(N)`. Per-bundle nonce sharding — all elements in one bundle go to the same shard. Better mapper-side pre-combine (PGBK) table efficiency. | When upstream provides many small bundles, or for moderate throughput where PGBK efficiency matters. |
| `precombine` | Mapper-side pre-aggregation within each bundle before shuffle. Reduces shuffle volume by folding values per key locally (like PGBKCVOperation) while preserving Dataflow incremental state-side merging on the downstream Combine. | Reducing shuffle volume when per-key cardinality is moderate and bundles are large. |
| `none` | Plain `Combine` with no fanout. Relies on Dataflow's combiner lifting (PGBK) for mapper-side pre-combining. | Low throughput, or when upstream already provides enough parallel bundles (e.g., `decompress_shards`) and streaming state I/O is not a bottleneck. |

## Getting Started

### Requirements

* Java 17
* Maven
* Python 3.11
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

### Required IAM Permissions

The **worker service account** (used by Dataflow workers) needs:

| Role | Reason |
|---|---|
| `roles/storage.objectAdmin` | Read/write GCS for staging artifacts and temp files (see note below) |
| `roles/dataflow.developer` | Create and manage Dataflow jobs |
| `roles/bigquery.dataOwner` | Create/delete temp datasets and tables, read CDC data |
| `roles/bigquery.jobUser` | Run BigQuery query jobs |
| `roles/pubsub.editor` | Publish anomaly alerts and verify topic exists |

The **user or CI account** that launches the template also needs
`roles/iam.serviceAccountUser` on the worker service account to impersonate it.

> **Note:** If you pre-create the temp dataset with `--temp_dataset`, you can
> scope `roles/bigquery.dataOwner` to just the source and temp datasets
> instead of project-wide, and use `roles/bigquery.dataEditor` if dataset
> deletion is not needed.

> **Note:** Dataflow auto-creates a default staging bucket
> (`dataflow-staging-{region}-{project_number}`) on first use in a region.
> If this bucket does not exist, the service account needs
> `roles/storage.admin` (or the bucket must be pre-created). Once the
> bucket exists, `roles/storage.objectAdmin` is sufficient.

### Building the Plugins

The Maven plugins must be installed before staging:

```shell
mvn install -pl plugins/core-plugin,plugins/templates-maven-plugin -am -DskipTests -q
```

### Staging the Template

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>

mvn clean package -PtemplatesStage \
  -DskipTests \
  -DprojectId="$PROJECT" \
  -DbucketName="$BUCKET_NAME" \
  -DstagePrefix="templates" \
  -DtemplateName="BigQuery_Anomaly_Detection" \
  -pl python
```

This builds the Docker image, pushes it to `gcr.io/$PROJECT/bigquery-anomaly-detection`,
and writes the template spec to `gs://$BUCKET_NAME/templates/flex/BigQuery_Anomaly_Detection`.

### Running the Template

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

gcloud dataflow flex-template run "bq-anomaly-$(date +%Y%m%d-%H%M%S)" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "gs://$BUCKET_NAME/templates/flex/BigQuery_Anomaly_Detection" \
  --parameters table="$PROJECT:my_dataset.my_table" \
  --parameters metric_spec='{"aggregation":{"window":{"type":"fixed","size_seconds":60},"measures":[{"field":"amount","agg":"SUM","alias":"revenue"}]}}' \
  --parameters detector_spec='{"type":"ZScore"}' \
  --parameters topic="projects/$PROJECT/topics/bqmonitor-anomalies" \
  --parameters duration_sec="300"
```

Or run directly from the container image (skipping the GCS spec file):

```shell
gcloud dataflow flex-template run "bq-anomaly-test" \
  --image "gcr.io/$PROJECT/bigquery-anomaly-detection:templates" \
  --project "$PROJECT" \
  --region "$REGION" \
  --sdk-language PYTHON \
  --parameters table="$PROJECT:my_dataset.my_table" \
  --parameters metric_spec='{"aggregation":{"window":{"type":"fixed","size_seconds":60},"measures":[{"field":"amount","agg":"SUM","alias":"revenue"}]}}' \
  --parameters detector_spec='{"type":"ZScore"}'
```

### Regenerating Pinned Dependencies

The `requirements_all.txt` contains pinned and hashed dependencies. To regenerate
after changing base dependencies:

```shell
# Edit python/default_base_bqmonitor_requirements.txt, then:
sh python/generate_all_dependencies.sh
```

### Running Integration Tests

The integration tests stage the template, launch it against real BigQuery and
Pub/Sub resources, and verify end-to-end anomaly detection.

```shell
export PROJECT=<my-project>
export REGION=us-east5
export BUCKET_NAME=<bucket-name>

# Build plugins first (one-time).
mvn install -pl plugins/core-plugin,plugins/templates-maven-plugin -am -DskipTests -q

# Run the integration tests.
mvn verify -PtemplatesIntegrationTests \
  -Dproject="$PROJECT" \
  -Dregion="$REGION" \
  -DartifactBucket="gs://$BUCKET_NAME" \
  -pl python \
  -Dtest=BigQueryAnomalyDetectionIT
```

To run a single test method:

```shell
mvn verify -PtemplatesIntegrationTests \
  -Dproject="$PROJECT" \
  -Dregion="$REGION" \
  -DartifactBucket="gs://$BUCKET_NAME" \
  -pl python \
  -Dtest=BigQueryAnomalyDetectionIT#testDetectsAnomalyAndPublishesToPubSub
```

The test service account needs all roles listed in
[Required IAM Permissions](#required-iam-permissions) plus the ability to
create and delete test resources (Pub/Sub topics/subscriptions, BigQuery
datasets/tables).

## Examples

### Simple SUM metric

```shell
--parameters metric_spec='{"aggregation":{"window":{"type":"fixed","size_seconds":3600},"measures":[{"field":"transaction_amount","agg":"SUM","alias":"revenue"}]}}'
--parameters detector_spec='{"type":"ZScore"}'
```

### Grouped ratio metric (CTR)

```shell
--parameters metric_spec='{"aggregation":{"window":{"type":"fixed","size_seconds":60},"group_by":["campaign_type","browser"],"measures":[{"field":"is_click","agg":"SUM","alias":"clicks"},{"field":"is_click","agg":"COUNT","alias":"impressions"}]},"measure_combiner":{"expression":"clicks / impressions"}}'
--parameters detector_spec='{"type":"ZScore"}'
```

### Derived field + custom threshold

```shell
--parameters metric_spec='{"derived_fields":[{"name":"is_success","expression":"1 if status == '"'"'success'"'"' else 0"}],"aggregation":{"window":{"type":"fixed","size_seconds":60},"group_by":["brand"],"measures":[{"field":"is_success","agg":"SUM","alias":"successes"},{"field":"is_success","agg":"COUNT","alias":"total"}]},"measure_combiner":{"expression":"successes / total"}}'
--parameters detector_spec='{"type":"ZScore","config":{"threshold_criterion":{"type":"FixedThreshold","config":{"cutoff":10}}}}'
```

## Project Structure

```
python/src/main/python/bigquery-anomaly-detection/
  main.py                    # Entry point
  setup.py                   # Package configuration
  pyproject.toml             # Build system config
  requirements_all.txt       # Pinned dependencies (generated)
  src/bqmonitor/
    __init__.py
    pipeline.py              # Pipeline construction and options
    cdc.py                   # BigQuery CDC reader (ReadBigQueryChangeHistory)
    metric.py                # MetricSpec and ComputeMetric PTransform
    safe_eval.py             # Safe expression evaluation (Expr)

python/src/main/java/.../BigQueryAnomalyDetection.java   # Template metadata
python/src/test/java/.../BigQueryAnomalyDetectionIT.java  # Integration test
python/default_base_bqmonitor_requirements.txt            # Base dependencies
```
