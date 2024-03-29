

# Autogenerated file. DO NOT EDIT.
#
# Copyright (C) 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#


variable "on_delete" {
  type        = string
  description = "One of \"drain\" or \"cancel\". Specifies behavior of deletion during terraform destroy."
}

variable "project" {
  type        = string
  description = "The Google Cloud Project ID within which this module provisions resources."
}

variable "region" {
  type        = string
  description = "The region in which the created job should run."
}

variable "inputFilePattern" {
  type        = string
  description = "This is the file location for Datastream file output in Cloud Storage. Normally, this will be gs://${BUCKET}/${ROOT_PATH}/."

}

variable "inputFileFormat" {
  type        = string
  description = "This is the format of the output file produced by Datastream. By default this will be avro."
  default     = null
}

variable "sessionFilePath" {
  type        = string
  description = "Session file path in Cloud Storage that contains mapping information from HarbourBridge"
  default     = null
}

variable "instanceId" {
  type        = string
  description = "This is the name of the Cloud Spanner instance where the changes are replicated."

}

variable "databaseId" {
  type        = string
  description = "This is the name of the Cloud Spanner database where the changes are replicated."

}

variable "projectId" {
  type        = string
  description = "This is the name of the Cloud Spanner project."
  default     = null
}

variable "spannerHost" {
  type        = string
  description = "The Cloud Spanner endpoint to call in the template. (Example: https://batch-spanner.googleapis.com). Defaults to: https://batch-spanner.googleapis.com."
  default     = null
}

variable "gcsPubSubSubscription" {
  type        = string
  description = "The Pub/Sub subscription being used in a Cloud Storage notification policy. The name should be in the format of projects/<project-id>/subscriptions/<subscription-name>."
  default     = null
}

variable "streamName" {
  type        = string
  description = "This is the Datastream stream name used to get information."

}

variable "shadowTablePrefix" {
  type        = string
  description = "The prefix used for the shadow table. Defaults to: shadow_."
  default     = null
}

variable "shouldCreateShadowTables" {
  type        = bool
  description = "This flag indicates whether shadow tables must be created in Cloud Spanner database. Defaults to: true."
  default     = null
}

variable "rfcStartDateTime" {
  type        = string
  description = "The starting DateTime used to fetch from Cloud Storage (https://tools.ietf.org/html/rfc3339). Defaults to: 1970-01-01T00:00:00.00Z."
  default     = null
}

variable "fileReadConcurrency" {
  type        = number
  description = "The number of concurrent DataStream files to read. Defaults to: 30."
  default     = null
}

variable "deadLetterQueueDirectory" {
  type        = string
  description = "This is the file path to store the deadletter queue output. Default is a directory under the Dataflow job's temp location. The default value is enough under most conditions."
  default     = null
}

variable "dlqRetryMinutes" {
  type        = number
  description = "The number of minutes between dead letter queue retries. Defaults to 10."
  default     = null
}

variable "dlqMaxRetryCount" {
  type        = number
  description = "The max number of times temporary errors can be retried through DLQ. Defaults to 500."
  default     = null
}

variable "dataStreamRootUrl" {
  type        = string
  description = "Datastream API Root URL. Defaults to: https://datastream.googleapis.com/."
  default     = null
}

variable "datastreamSourceType" {
  type        = string
  description = "This is the type of source database that Datastream connects to. Example - mysql/oracle. Need to be set when testing without an actual running Datastream."
  default     = null
}

variable "roundJsonDecimals" {
  type        = bool
  description = "This flag if set, rounds the decimal values in json columns to a number that can be stored without loss of precision. Defaults to: false."
  default     = null
}

variable "runMode" {
  type        = string
  description = "This is the run mode type, whether regular or with retryDLQ. Defaults to: regular."
  default     = null
}

variable "transformationContextFilePath" {
  type        = string
  description = "Transformation context file path in cloud storage used to populate data used in transformations performed during migrations   Eg: The shard id to db name to identify the db from which a row was migrated"
  default     = null
}

variable "directoryWatchDurationInMinutes" {
  type        = number
  description = "The Duration for which the pipeline should keep polling a directory in GCS. Datastreamoutput files are arranged in a directory structure which depicts the timestamp of the event grouped by minutes. This parameter should be approximately equal tomaximum delay which could occur between event occurring in source database and the same event being written to GCS by Datastream. 99.9 percentile = 10 minutes. Defaults to: 10."
  default     = null
}

variable "spannerPriority" {
  type        = string
  description = "The request priority for Cloud Spanner calls. The value must be one of: [HIGH,MEDIUM,LOW]. Defaults to HIGH"
  default     = null
}

variable "dlqGcsPubSubSubscription" {
  type        = string
  description = "The Pub/Sub subscription being used in a Cloud Storage notification policy for DLQ retry directory when running in regular mode. The name should be in the format of projects/<project-id>/subscriptions/<subscription-name>. When set, the deadLetterQueueDirectory and dlqRetryMinutes are ignored."
  default     = null
}


provider "google" {
  project = var.project
}

provider "google-beta" {
  project = var.project
}

variable "additional_experiments" {
  type        = set(string)
  description = "List of experiments that should be used by the job. An example value is  'enable_stackdriver_agent_metrics'."
  default     = null
}

variable "autoscaling_algorithm" {
  type        = string
  description = "The algorithm to use for autoscaling"
  default     = null
}

variable "enable_streaming_engine" {
  type        = bool
  description = "Indicates if the job should use the streaming engine feature."
  default     = null
}

variable "ip_configuration" {
  type        = string
  description = "The configuration for VM IPs. Options are 'WORKER_IP_PUBLIC' or 'WORKER_IP_PRIVATE'."
  default     = null
}

variable "kms_key_name" {
  type        = string
  description = "The name for the Cloud KMS key for the job. Key format is: projects/PROJECT_ID/locations/LOCATION/keyRings/KEY_RING/cryptoKeys/KEY"
  default     = null
}

variable "labels" {
  type        = map(string)
  description = "User labels to be specified for the job. Keys and values should follow the restrictions specified in the labeling restrictions page. NOTE: This field is non-authoritative, and will only manage the labels present in your configuration.				Please refer to the field 'effective_labels' for all of the labels present on the resource."
  default     = null
}

variable "launcher_machine_type" {
  type        = string
  description = "The machine type to use for launching the job. The default is n1-standard-1."
  default     = null
}

variable "machine_type" {
  type        = string
  description = "The machine type to use for the job."
  default     = null
}

variable "max_workers" {
  type        = number
  description = "The maximum number of Google Compute Engine instances to be made available to your pipeline during execution, from 1 to 1000."
  default     = null
}

variable "name" {
  type = string
}

variable "network" {
  type        = string
  description = "The network to which VMs will be assigned. If it is not provided, 'default' will be used."
  default     = null
}

variable "num_workers" {
  type        = number
  description = "The initial number of Google Compute Engine instances for the job."
  default     = null
}

variable "sdk_container_image" {
  type        = string
  description = "Docker registry location of container image to use for the 'worker harness. Default is the container for the version of the SDK. Note this field is only valid for portable pipelines."
  default     = null
}

variable "service_account_email" {
  type        = string
  description = "The Service Account email used to create the job."
  default     = null
}

variable "skip_wait_on_job_termination" {
  type        = bool
  description = "If true, treat DRAINING and CANCELLING as terminal job states and do not wait for further changes before removing from terraform state and moving on. WARNING: this will lead to job name conflicts if you do not ensure that the job names are different, e.g. by embedding a release ID or by using a random_id."
  default     = null
}

variable "staging_location" {
  type        = string
  description = "The Cloud Storage path to use for staging files. Must be a valid Cloud Storage URL, beginning with gs://."
  default     = null
}

variable "subnetwork" {
  type        = string
  description = "The subnetwork to which VMs will be assigned. Should be of the form 'regions/REGION/subnetworks/SUBNETWORK'."
  default     = null
}

variable "temp_location" {
  type        = string
  description = "The Cloud Storage path to use for temporary files. Must be a valid Cloud Storage URL, beginning with gs://."
  default     = null
}

resource "google_project_service" "required" {
  service            = "dataflow.googleapis.com"
  disable_on_destroy = false
}

resource "google_dataflow_flex_template_job" "generated" {
  depends_on              = [google_project_service.required]
  provider                = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Cloud_Datastream_to_Spanner"
  parameters = {
    inputFilePattern                = var.inputFilePattern
    inputFileFormat                 = var.inputFileFormat
    sessionFilePath                 = var.sessionFilePath
    instanceId                      = var.instanceId
    databaseId                      = var.databaseId
    projectId                       = var.projectId
    spannerHost                     = var.spannerHost
    gcsPubSubSubscription           = var.gcsPubSubSubscription
    streamName                      = var.streamName
    shadowTablePrefix               = var.shadowTablePrefix
    shouldCreateShadowTables        = tostring(var.shouldCreateShadowTables)
    rfcStartDateTime                = var.rfcStartDateTime
    fileReadConcurrency             = tostring(var.fileReadConcurrency)
    deadLetterQueueDirectory        = var.deadLetterQueueDirectory
    dlqRetryMinutes                 = tostring(var.dlqRetryMinutes)
    dlqMaxRetryCount                = tostring(var.dlqMaxRetryCount)
    dataStreamRootUrl               = var.dataStreamRootUrl
    datastreamSourceType            = var.datastreamSourceType
    roundJsonDecimals               = tostring(var.roundJsonDecimals)
    runMode                         = var.runMode
    transformationContextFilePath   = var.transformationContextFilePath
    directoryWatchDurationInMinutes = tostring(var.directoryWatchDurationInMinutes)
    spannerPriority                 = var.spannerPriority
    dlqGcsPubSubSubscription        = var.dlqGcsPubSubSubscription
  }

  additional_experiments       = var.additional_experiments
  autoscaling_algorithm        = var.autoscaling_algorithm
  enable_streaming_engine      = var.enable_streaming_engine
  ip_configuration             = var.ip_configuration
  kms_key_name                 = var.kms_key_name
  labels                       = var.labels
  launcher_machine_type        = var.launcher_machine_type
  machine_type                 = var.machine_type
  max_workers                  = var.max_workers
  name                         = var.name
  network                      = var.network
  num_workers                  = var.num_workers
  sdk_container_image          = var.sdk_container_image
  service_account_email        = var.service_account_email
  skip_wait_on_job_termination = var.skip_wait_on_job_termination
  staging_location             = var.staging_location
  subnetwork                   = var.subnetwork
  temp_location                = var.temp_location
  region                       = var.region
}

output "dataflow_job_url" {
  value = "https://console.cloud.google.com/dataflow/jobs/${var.region}/${google_dataflow_flex_template_job.generated.job_id}"
}

