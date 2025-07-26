# Managed I/O to Managed I/O Template

The [ManagedIOToManagedIO](src/main/java/com/google/cloud/teleport/v2/templates/ManagedIOToManagedIO.java)
template is a flexible pipeline that can read from any Managed I/O source and write to any Managed I/O sink.
This template supports all available Managed I/O connectors including ICEBERG, ICEBERG_CDC, KAFKA, and BIGQUERY.
The template automatically determines whether to run in streaming or batch mode based on the source connector type
(ICEBERG_CDC and KAFKA use streaming, others use batch). The template uses Apache Beam's Managed API to provide
a unified interface for configuring different I/O connectors through simple configuration maps.

## Getting Started

### Requirements

* Java 11
* Maven
* The source and sink configurations must be valid for the specified connector types
* Required permissions for accessing the source and sink systems

### Template Parameters

| Parameter | Description |
|-----------|-------------|
| sourceConnectorType | The type of Managed I/O connector to use as source. Supported values: ICEBERG, ICEBERG_CDC, KAFKA, BIGQUERY. |
| sourceConfig | JSON configuration for the source Managed I/O connector. The configuration format depends on the connector type. |
| sinkConnectorType | The type of Managed I/O connector to use as sink. Supported values: ICEBERG, KAFKA, BIGQUERY. Note: ICEBERG_CDC is only available for reading. |
| sinkConfig | JSON configuration for the sink Managed I/O connector. The configuration format depends on the connector type. |

## Supported Managed I/O Connectors

### Source Connectors

#### ICEBERG
Reads from Apache Iceberg tables.

**Example Configuration:**
```json
{
  "table": "my_catalog.my_database.my_table",
  "catalog_name": "my_catalog",
  "catalog_properties": {
    "warehouse": "gs://my-bucket/warehouse",
    "catalog-impl": "org.apache.iceberg.gcp.biglake.BigLakeCatalog"
  }
}
```

#### ICEBERG_CDC
Reads change data capture (CDC) streams from Apache Iceberg tables.

**Example Configuration:**
```json
{
  "table": "my_catalog.my_database.my_table",
  "catalog_name": "my_catalog",
  "streaming": true,
  "starting_strategy": "latest"
}
```

#### KAFKA
Reads from Apache Kafka topics.

**Example Configuration:**
```json
{
  "bootstrap_servers": "localhost:9092",
  "topic": "input-topic",
  "format": "JSON"
}
```

#### BIGQUERY
Reads from Google BigQuery tables.

**Example Configuration:**
```json
{
  "table": "project:dataset.table",
  "query": "SELECT * FROM `project.dataset.table` WHERE date >= '2024-01-01'"
}
```

### Sink Connectors

#### ICEBERG
Writes to Apache Iceberg tables.

**Example Configuration:**
```json
{
  "table": "my_catalog.my_database.output_table",
  "catalog_name": "my_catalog",
  "catalog_properties": {
    "warehouse": "gs://my-bucket/warehouse",
    "catalog-impl": "org.apache.iceberg.gcp.biglake.BigLakeCatalog"
  }
}
```

#### KAFKA
Writes to Apache Kafka topics.

**Example Configuration:**
```json
{
  "bootstrap_servers": "localhost:9092",
  "topic": "output-topic",
  "format": "JSON"
}
```

#### BIGQUERY
Writes to Google BigQuery tables.

**Example Configuration:**
```json
{
  "table": "project:dataset.output_table"
}
```

## Usage Examples

### Example 1: Kafka to BigQuery

```bash
gcloud dataflow flex-template run "managed-io-kafka-to-bigquery" \
    --template-file-gcs-location="gs://dataflow-templates/latest/flex/Managed_IO_to_Managed_IO" \
    --region="us-central1" \
    --parameters="sourceConnectorType=KAFKA" \
    --parameters="sourceConfig={\"bootstrap_servers\": \"localhost:9092\", \"topic\": \"input-topic\", \"format\": \"JSON\"}" \
    --parameters="sinkConnectorType=BIGQUERY" \
    --parameters="sinkConfig={\"table\": \"my-project:my_dataset.my_table\"}"
```

### Example 2: BigQuery to Iceberg

```bash
gcloud dataflow flex-template run "managed-io-bigquery-to-iceberg" \
    --template-file-gcs-location="gs://dataflow-templates/latest/flex/Managed_IO_to_Managed_IO" \
    --region="us-central1" \
    --parameters="sourceConnectorType=BIGQUERY" \
    --parameters="sourceConfig={\"table\": \"my-project:my_dataset.source_table\"}" \
    --parameters="sinkConnectorType=ICEBERG" \
    --parameters="sinkConfig={\"table\": \"my_catalog.my_database.target_table\", \"catalog_name\": \"my_catalog\"}"
```

### Example 3: Iceberg CDC to Kafka (Automatically Streaming)

```bash
gcloud dataflow flex-template run "managed-io-iceberg-cdc-to-kafka" \
    --template-file-gcs-location="gs://dataflow-templates/latest/flex/Managed_IO_to_Managed_IO" \
    --region="us-central1" \
    --parameters="sourceConnectorType=ICEBERG_CDC" \
    --parameters="sourceConfig={\"table\": \"my_catalog.my_database.my_table\", \"streaming\": true}" \
    --parameters="sinkConnectorType=KAFKA" \
    --parameters="sinkConfig={\"bootstrap_servers\": \"localhost:9092\", \"topic\": \"output-topic\"}"
```

## Building the Template

### Staging the Template

If you are using Google Cloud Shell, you can run this script in Cloud Shell directly.

```shell
export PROJECT=<my-project>
export BUCKET_NAME=gs://<bucket-name>

./cicd/run-build.sh --template-module managed-io-to-managed-io --stage-only
```

The template file will be staged at `gs://<bucket-name>/templates/flex/Managed_IO_to_Managed_IO`.

### Executing the Template

The template requires the following parameters:

* **sourceConnectorType**: Source connector type (ICEBERG, ICEBERG_CDC, KAFKA, BIGQUERY)
* **sourceConfig**: JSON configuration for the source connector
* **sinkConnectorType**: Sink connector type (ICEBERG, KAFKA, BIGQUERY)
* **sinkConfig**: JSON configuration for the sink connector

The template automatically determines streaming vs. batch mode based on the source connector type:
* **Streaming mode**: ICEBERG_CDC, KAFKA
* **Batch mode**: ICEBERG, BIGQUERY

The template can then be executed using gcloud:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=gs://<bucket-name>
export REGION=us-central1

### Required
export SOURCE_CONNECTOR_TYPE=<sourceConnectorType>
export SOURCE_CONFIG=<sourceConfig>
export SINK_CONNECTOR_TYPE=<sinkConnectorType>
export SINK_CONFIG=<sinkConfig>

gcloud dataflow flex-template run "managed-io-to-managed-io-job" \
  --template-file-gcs-location="$BUCKET_NAME/templates/flex/Managed_IO_to_Managed_IO" \
  --region="$REGION" \
  --parameters="sourceConnectorType=$SOURCE_CONNECTOR_TYPE" \
  --parameters="sourceConfig=$SOURCE_CONFIG" \
  --parameters="sinkConnectorType=$SINK_CONNECTOR_TYPE" \
  --parameters="sinkConfig=$SINK_CONFIG"
```

## Configuration Reference

For detailed configuration options for each Managed I/O connector, please refer to the
[Apache Beam Managed I/O documentation](https://beam.apache.org/documentation/io/managed-io/).

## Troubleshooting

### Common Issues

1. **Invalid JSON Configuration**: Ensure that the sourceConfig and sinkConfig parameters contain valid JSON.
2. **Unsupported Connector Combinations**: Some connector combinations may not be supported in streaming mode.
3. **Permission Issues**: Ensure that the Dataflow service account has the necessary permissions to access the source and sink systems.
4. **ICEBERG_CDC for Sinks**: ICEBERG_CDC is only available for reading, not writing.

### Logging

The template uses standard Apache Beam logging. You can view logs in the Google Cloud Console under Dataflow > Jobs > [Your Job] > Logs.
