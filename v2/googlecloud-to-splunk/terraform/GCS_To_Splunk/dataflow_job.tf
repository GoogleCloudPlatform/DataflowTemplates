

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

variable "invalidOutputPath" {
  type        = string
  description = "Cloud Storage path where to write objects that could not be converted to Splunk objects or pushed to Splunk. (Example: gs://your-bucket/your-path)"

}

variable "inputFileSpec" {
  type        = string
  description = "Cloud storage file pattern glob to read from. ex: gs://your-bucket/path/*.csv"

}

variable "containsHeaders" {
  type        = bool
  description = "Input CSV files contain a header record (true/false). Only required if reading CSV files. Defaults to: false."
  default     = null
}

variable "deadletterTable" {
  type        = string
  description = "Messages failed to reach the target for all kind of reasons (e.g., mismatched schema, malformed json) are written to this table. (Example: your-project:your-dataset.your-table-name)"

}

variable "delimiter" {
  type        = string
  description = "The column delimiter of the input text files. Default: use delimiter provided in csvFormat (Example: ,)"
  default     = null
}

variable "csvFormat" {
  type        = string
  description = "CSV format specification to use for parsing records. Default is: Default. See https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html for more details. Must match format names exactly found at: https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.Predefined.html"
  default     = null
}

variable "jsonSchemaPath" {
  type        = string
  description = "Path to JSON schema. Default: null. (Example: gs://path/to/schema)"
  default     = null
}

variable "largeNumFiles" {
  type        = bool
  description = "Set to true if number of files is in the tens of thousands. Defaults to: false."
  default     = null
}

variable "csvFileEncoding" {
  type        = string
  description = "CSV file character encoding format. Allowed Values are US-ASCII, ISO-8859-1, UTF-8, UTF-16. Defaults to: UTF-8."
  default     = null
}

variable "logDetailedCsvConversionErrors" {
  type        = bool
  description = "Set to true to enable detailed error logging when CSV parsing fails. Note that this may expose sensitive data in the logs (e.g., if the CSV file contains passwords). Default: false."
  default     = null
}

variable "token" {
  type        = string
  description = "Splunk Http Event Collector (HEC) authentication token. Must be provided if the tokenSource is set to PLAINTEXT or KMS."
  default     = null
}

variable "url" {
  type        = string
  description = "Splunk Http Event Collector (HEC) url. This should be routable from the VPC in which the pipeline runs. (Example: https://splunk-hec-host:8088)"

}

variable "batchCount" {
  type        = number
  description = "Batch size for sending multiple events to Splunk HEC. Default 1 (no batching)."
  default     = null
}

variable "disableCertificateValidation" {
  type        = bool
  description = "Disable SSL certificate validation (true/false). Default false (validation enabled). If true, the certificates are not validated (all certificates are trusted) and  `rootCaCertificatePath` parameter is ignored."
  default     = null
}

variable "parallelism" {
  type        = number
  description = "Maximum number of parallel requests. Default: 1 (no parallelism)."
  default     = null
}

variable "tokenSource" {
  type        = string
  description = "Source of the token. One of PLAINTEXT, KMS or SECRET_MANAGER. If tokenSource is set to KMS, tokenKMSEncryptionKey and encrypted token must be provided. If tokenSource is set to SECRET_MANAGER, tokenSecretId must be provided. If tokenSource is set to PLAINTEXT, token must be provided."

}

variable "tokenKMSEncryptionKey" {
  type        = string
  description = "The Cloud KMS key to decrypt the HEC token string. This parameter must be provided if the tokenSource is set to KMS. If this parameter is provided, token string should be passed in encrypted. Encrypt parameters using the KMS API encrypt endpoint. The Key should be in the format projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}. See: https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt  (Example: projects/your-project-id/locations/global/keyRings/your-keyring/cryptoKeys/your-key-name)"
  default     = null
}

variable "tokenSecretId" {
  type        = string
  description = "Secret Manager secret ID for the token. This parameter should be provided if the tokenSource is set to SECRET_MANAGER. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}. (Example: projects/your-project-id/secrets/your-secret/versions/your-secret-version)"
  default     = null
}

variable "rootCaCertificatePath" {
  type        = string
  description = "The full URL to root CA certificate in Cloud Storage. The certificate provided in Cloud Storage must be DER-encoded and may be supplied in binary or printable (Base64) encoding. If the certificate is provided in Base64 encoding, it must be bounded at the beginning by -----BEGIN CERTIFICATE-----, and must be bounded at the end by -----END CERTIFICATE-----. If this parameter is provided, this private CA certificate file will be fetched and added to Dataflow worker's trust store in order to verify Splunk HEC endpoint's SSL certificate which is signed by that private CA. If this parameter is not provided, the default trust store is used. (Example: gs://mybucket/mycerts/privateCA.crt)"
  default     = null
}

variable "enableBatchLogs" {
  type        = bool
  description = "Parameter which specifies if logs should be enabled for batches written to Splunk. Defaults to: true."
  default     = null
}

variable "enableGzipHttpCompression" {
  type        = bool
  description = "Parameter which specifies if HTTP requests sent to Splunk HEC should be GZIP encoded. Defaults to: true."
  default     = null
}

variable "javascriptTextTransformGcsPath" {
  type        = string
  description = "The Cloud Storage path pattern for the JavaScript code containing your user-defined functions. (Example: gs://your-bucket/your-function.js)"
  default     = null
}

variable "javascriptTextTransformFunctionName" {
  type        = string
  description = "The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: 'transform' or 'transform_udf1')"
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
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/GCS_To_Splunk"
  parameters = {
    invalidOutputPath                   = var.invalidOutputPath
    inputFileSpec                       = var.inputFileSpec
    containsHeaders                     = tostring(var.containsHeaders)
    deadletterTable                     = var.deadletterTable
    delimiter                           = var.delimiter
    csvFormat                           = var.csvFormat
    jsonSchemaPath                      = var.jsonSchemaPath
    largeNumFiles                       = tostring(var.largeNumFiles)
    csvFileEncoding                     = var.csvFileEncoding
    logDetailedCsvConversionErrors      = tostring(var.logDetailedCsvConversionErrors)
    token                               = var.token
    url                                 = var.url
    batchCount                          = tostring(var.batchCount)
    disableCertificateValidation        = tostring(var.disableCertificateValidation)
    parallelism                         = tostring(var.parallelism)
    tokenSource                         = var.tokenSource
    tokenKMSEncryptionKey               = var.tokenKMSEncryptionKey
    tokenSecretId                       = var.tokenSecretId
    rootCaCertificatePath               = var.rootCaCertificatePath
    enableBatchLogs                     = tostring(var.enableBatchLogs)
    enableGzipHttpCompression           = tostring(var.enableGzipHttpCompression)
    javascriptTextTransformGcsPath      = var.javascriptTextTransformGcsPath
    javascriptTextTransformFunctionName = var.javascriptTextTransformFunctionName
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

