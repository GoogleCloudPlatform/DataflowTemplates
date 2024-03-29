

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

variable "inputSubscription" {
  type        = string
  description = "Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name' (Example: projects/your-project-id/subscriptions/your-subscription-name)"

}

variable "dataset" {
  type        = string
  description = "The type of logs sent via Pub/Sub for which we have out of the box dashboard. Known log types values are audit, vpcflow, and firewall. If no known log type is detected, we default to 'pubsub'"
  default     = null
}

variable "namespace" {
  type        = string
  description = "The namespace for dataset. Default is default"
  default     = null
}

variable "errorOutputTopic" {
  type        = string
  description = "The Pub/Sub topic to publish deadletter records to. The name should be in the format of `projects/your-project-id/topics/your-topic-name`."

}

variable "elasticsearchTemplateVersion" {
  type        = string
  description = "Dataflow Template Version Identifier, usually defined by Google Cloud. Defaults to: 1.0.0."
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

variable "javascriptTextTransformReloadIntervalMinutes" {
  type        = number
  description = "Define the interval that workers may check for JavaScript UDF changes to reload the files. Defaults to: 0."
  default     = null
}

variable "connectionUrl" {
  type        = string
  description = "Elasticsearch URL in the format https://hostname:[port] or specify CloudID if using Elastic Cloud (Example: https://elasticsearch-host:9200)"

}

variable "apiKey" {
  type        = string
  description = "Base64 Encoded API Key for access without requiring basic authentication. Refer to: https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html#security-api-create-api-key-request"

}

variable "elasticsearchUsername" {
  type        = string
  description = "Username for Elasticsearch endpoint. Overrides ApiKey option if specified"
  default     = null
}

variable "elasticsearchPassword" {
  type        = string
  description = "Password for Elasticsearch endpoint. Overrides ApiKey option if specified"
  default     = null
}

variable "batchSize" {
  type        = number
  description = "Batch Size used for batch insertion of messages into Elasticsearch. Defaults to: 1000."
  default     = null
}

variable "batchSizeBytes" {
  type        = number
  description = "Batch Size in bytes used for batch insertion of messages into elasticsearch. Default: 5242880 (5mb)"
  default     = null
}

variable "maxRetryAttempts" {
  type        = number
  description = "Max retry attempts, must be > 0. Default: no retries"
  default     = null
}

variable "maxRetryDuration" {
  type        = number
  description = "Max retry duration in milliseconds, must be > 0. Default: no retries"
  default     = null
}

variable "propertyAsIndex" {
  type        = string
  description = "A property in the document being indexed whose value will specify _index metadata to be included with document in bulk request (takes precedence over an _index UDF)."
  default     = null
}

variable "javaScriptIndexFnGcsPath" {
  type        = string
  description = "Cloud Storage path to JavaScript UDF source for function that will specify _index metadata to be included with document in bulk request."
  default     = null
}

variable "javaScriptIndexFnName" {
  type        = string
  description = "UDF JavaScript Function Name for function that will specify _index metadata to be included with document in bulk request"
  default     = null
}

variable "propertyAsId" {
  type        = string
  description = "A property in the document being indexed whose value will specify _id metadata to be included with document in bulk request (takes precedence over an _id UDF)."
  default     = null
}

variable "javaScriptIdFnGcsPath" {
  type        = string
  description = "Cloud Storage path to JavaScript UDF source for function that will specify _id metadata to be included with document in bulk request."
  default     = null
}

variable "javaScriptIdFnName" {
  type        = string
  description = "UDF JavaScript Function Name for function that will specify _id metadata to be included with document in bulk request."
  default     = null
}

variable "javaScriptTypeFnGcsPath" {
  type        = string
  description = "Cloud Storage path to JavaScript UDF source for function that will specify _type metadata to be included with document in bulk request."
  default     = null
}

variable "javaScriptTypeFnName" {
  type        = string
  description = "UDF JavaScript Function Name for function that will specify _type metadata to be included with document in bulk request"
  default     = null
}

variable "javaScriptIsDeleteFnGcsPath" {
  type        = string
  description = <<EOT
Cloud Storage path to JavaScript UDF source for function that will determine if document should be deleted rather than inserted or updated, function should return string value "true" or "false".
EOT
  default     = null
}

variable "javaScriptIsDeleteFnName" {
  type        = string
  description = <<EOT
UDF JavaScript Function Name for function that will determine if document should be deleted rather than inserted or updated, function should return string value "true" or "false".
EOT
  default     = null
}

variable "usePartialUpdate" {
  type        = bool
  description = "Whether to use partial updates (update rather than create or index, allowing partial docs) with Elasticsearch requests. Defaults to: false."
  default     = null
}

variable "bulkInsertMethod" {
  type        = string
  description = "Whether to use INDEX (index, allows upsert) or CREATE (create, errors on duplicate _id) with Elasticsearch bulk requests. Defaults to: CREATE."
  default     = null
}

variable "trustSelfSignedCerts" {
  type        = bool
  description = "Whether to trust self-signed certificate or not. An Elasticsearch instance installed might have a self-signed certificate, Enable this to True to by-pass the validation on SSL certificate. (default is False)"
  default     = null
}

variable "disableCertificateValidation" {
  type        = bool
  description = "Disable SSL certificate validation (true/false). Default false (validation enabled). If true, all certificates are considered trusted."
  default     = null
}

variable "apiKeyKMSEncryptionKey" {
  type        = string
  description = "The Cloud KMS key to decrypt the API key. This parameter must be provided if the apiKeySource is set to KMS. If this parameter is provided, apiKey string should be passed in encrypted. Encrypt parameters using the KMS API encrypt endpoint. The Key should be in the format projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}. See: https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt  (Example: projects/your-project-id/locations/global/keyRings/your-keyring/cryptoKeys/your-key-name)"
  default     = null
}

variable "apiKeySecretId" {
  type        = string
  description = "Secret Manager secret ID for the apiKey. This parameter should be provided if the apiKeySource is set to SECRET_MANAGER. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}. (Example: projects/your-project-id/secrets/your-secret/versions/your-secret-version)"
  default     = null
}

variable "apiKeySource" {
  type        = string
  description = "Source of the API key. One of PLAINTEXT, KMS or SECRET_MANAGER. This parameter must be provided if secret manager or KMS is used. If apiKeySource is set to KMS, apiKeyKMSEncryptionKey and encrypted apiKey must be provided. If apiKeySource is set to SECRET_MANAGER, apiKeySecretId must be provided. If apiKeySource is set to PLAINTEXT, apiKey must be provided. Defaults to: PLAINTEXT."
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
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/PubSub_to_Elasticsearch"
  parameters = {
    inputSubscription                            = var.inputSubscription
    dataset                                      = var.dataset
    namespace                                    = var.namespace
    errorOutputTopic                             = var.errorOutputTopic
    elasticsearchTemplateVersion                 = var.elasticsearchTemplateVersion
    javascriptTextTransformGcsPath               = var.javascriptTextTransformGcsPath
    javascriptTextTransformFunctionName          = var.javascriptTextTransformFunctionName
    javascriptTextTransformReloadIntervalMinutes = tostring(var.javascriptTextTransformReloadIntervalMinutes)
    connectionUrl                                = var.connectionUrl
    apiKey                                       = var.apiKey
    elasticsearchUsername                        = var.elasticsearchUsername
    elasticsearchPassword                        = var.elasticsearchPassword
    batchSize                                    = tostring(var.batchSize)
    batchSizeBytes                               = tostring(var.batchSizeBytes)
    maxRetryAttempts                             = tostring(var.maxRetryAttempts)
    maxRetryDuration                             = tostring(var.maxRetryDuration)
    propertyAsIndex                              = var.propertyAsIndex
    javaScriptIndexFnGcsPath                     = var.javaScriptIndexFnGcsPath
    javaScriptIndexFnName                        = var.javaScriptIndexFnName
    propertyAsId                                 = var.propertyAsId
    javaScriptIdFnGcsPath                        = var.javaScriptIdFnGcsPath
    javaScriptIdFnName                           = var.javaScriptIdFnName
    javaScriptTypeFnGcsPath                      = var.javaScriptTypeFnGcsPath
    javaScriptTypeFnName                         = var.javaScriptTypeFnName
    javaScriptIsDeleteFnGcsPath                  = var.javaScriptIsDeleteFnGcsPath
    javaScriptIsDeleteFnName                     = var.javaScriptIsDeleteFnName
    usePartialUpdate                             = tostring(var.usePartialUpdate)
    bulkInsertMethod                             = var.bulkInsertMethod
    trustSelfSignedCerts                         = tostring(var.trustSelfSignedCerts)
    disableCertificateValidation                 = tostring(var.disableCertificateValidation)
    apiKeyKMSEncryptionKey                       = var.apiKeyKMSEncryptionKey
    apiKeySecretId                               = var.apiKeySecretId
    apiKeySource                                 = var.apiKeySource
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

