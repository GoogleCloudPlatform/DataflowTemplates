provider "google-beta" {
  project = var.project
}
variable "project" {
  default = "<my-project>"
}
variable "region" {
  default = "us-central1"
}

resource "google_dataflow_flex_template_job" "bigtable_change_streams_to_bigtable" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Bigtable_Change_Streams_to_Bigtable"
  name              = "bigtable-change-streams-to-bigtable"
  region            = var.region
  parameters        = {
    bigtableChangeStreamAppProfile = "<bigtableChangeStreamAppProfile>"
    bigtableReadInstanceId = "<bigtableReadInstanceId>"
    bigtableReadTableId = "<bigtableReadTableId>"
    bigtableWriteInstanceId = "<bigtableWriteInstanceId>"
    bigtableWriteTableId = "<bigtableWriteTableId>"
    bigtableWriteColumnFamily = "<bigtableWriteColumnFamily>"
    # bidirectionalReplicationEnabled = "false"
    # cbtQualifier = "BIDIRECTIONAL_REPL_SOURCE_CBT"
    # cbtFilterQualifier = "BIDIRECTIONAL_REPL_SOURCE_CBT"
    # dryRunEnabled = "false"
    # filterGCMutations = "false"
    # addRedistribute = "false"
    # bigtableChangeStreamMetadataInstanceId = ""
    # bigtableChangeStreamMetadataTableTableId = ""
    # bigtableChangeStreamCharset = "UTF-8"
    # bigtableChangeStreamStartTimestamp = ""
    # bigtableChangeStreamIgnoreColumnFamilies = ""
    # bigtableChangeStreamIgnoreColumns = ""
    # bigtableChangeStreamName = "<bigtableChangeStreamName>"
    # bigtableChangeStreamResume = "false"
    # bigtableReadChangeStreamTimeoutMs = "<bigtableReadChangeStreamTimeoutMs>"
    # bigtableReadProjectId = ""
    # bigtableReadAppProfile = "default"
    # bigtableRpcAttemptTimeoutMs = "<bigtableRpcAttemptTimeoutMs>"
    # bigtableRpcTimeoutMs = "<bigtableRpcTimeoutMs>"
    # bigtableAdditionalRetryCodes = "<bigtableAdditionalRetryCodes>"
    # bigtableWriteAppProfile = "default"
    # bigtableWriteProjectId = "<bigtableWriteProjectId>"
    # bigtableBulkWriteLatencyTargetMs = "<bigtableBulkWriteLatencyTargetMs>"
    # bigtableBulkWriteMaxRowKeyCount = "<bigtableBulkWriteMaxRowKeyCount>"
    # bigtableBulkWriteMaxRequestSizeBytes = "<bigtableBulkWriteMaxRequestSizeBytes>"
    # bigtableBulkWriteFlowControl = "false"
  }
}
