# GCS To Splunk Dataflow Template

The [GCSToSplunk](../../src/main/java/com/google/cloud/teleport/v2/templates/GCSToSplunk.java) pipeline ingests
data from CSV files stored in GCS, optionally executes a UDF,
and writes those records into Splunk's HEC endpoint.

The template creates the Splunk payload as a JSON element using one of the following:

1. CSV headers (default)
2. JSON schema
3. Javascript UDF


If a Javascript UDF and JSON schema are both inputted as parameters, 
only the Javascript UDF will be executed.


## Getting Started

### Requirements

* Java 11
* Maven
* Cloud Storage bucket exists
* Splunk HEC instance exists

### Building Template

This is a Flex Template meaning that the pipeline code will be containerized, and the container will be
run on Dataflow.

#### Set Environment Variables

```sh
export PROJECT=<my-project>
export REGION=us-central1
export IMAGE_NAME=<my-image-name>
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE=googlecloud-to-splunk
export APP_ROOT=/template/gcs-to-splunk 
export COMMAND_SPEC=${APP_ROOT}/resources/gcs-to-splunk-command-spec.json
```

#### Build and Push Image to Google Container Repository

```sh
mvn clean package -pl v2/googlecloud-to-splunk -am \
    -Dimage=${TARGET_GCR_IMAGE} \
    -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
    -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
    -Dapp-root=${APP_ROOT} \
    -Dcommand-spec=${COMMAND_SPEC}
```

#### Create Image Spec

Create a file with the metadata required for launching the Flex template. Once
created, this file should be placed in GCS.

The `gcs-to-splunk-image-spec.json` file in this directory
contains most of the content for this file. Simply update `image` property to
the value of `${TARGET_GCR_IMAGE}` defined earlier.

### Executing Template

The template requires the following parameters:

* inputFileSpec: Pattern of where the CSV file(s) are located in GCS, ex: gs://mybucket/somepath/*.csv
* invalidOutputPath: Google Cloud Storage path where to write objects that could not be pushed to Splunk. Ex: gs://mybucket/somepath
* tokenSource: Source of the token passed. One of PLAINTEXT, KMS or SECRET_MANAGER: If this is set to KMS, `tokenKMSEncryptionKey` and encrypted `token` must be provided. If this is set to SECRET_MANAGER, `tokenSecretId` must be provided. If this is set to PLAINTEXT, `token` must be provided.
* url: Splunk Http Event Collector (HEC) url. This should be routable from the VPC in which the pipeline runs. e.g. https://splunk-hec-host:8088

The template has the following optional parameters:
* containsHeaders: Set to "true" if CSV file contains headers, or "false" otherwise. An error is thrown if all files read from `inputFileSpec` do not follow the same header format. If set to "false", a JSON schema or Javascript UDF will need to be supplied. Default: false
* delimiter: Delimiting character in CSV file(s). Default: use delimiter found in `csvFormat` Example: ,
* csvFormat: CSV format according to [Apache Commons CSV format](https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html). Default is: DEFAULT
* batchCount: Batch size for sending multiple events to Splunk HEC. Default: 1 (no batching).
* parallelism: Maximum number of parallel requests. Default: 1 (no parallelism).
* disableCertificateValidation: Disable SSL certificate validation (true/false). Default: false (validation enabled). If true, the certificates are not validated (all certificates are trusted).
* token: Splunk Http Event Collector (HEC) authentication token. Must be set if `tokenSource` is set to either KMS or PLAINTEXT
* tokenKMSEncryptionKey: The Cloud KMS key to decrypt the HEC token string. This parameter must be provided if `tokenSource` is set to KMS. If this parameter is provided, the `token` parameter should be passed in encrypted. Encrypt parameters using the KMS API encrypt endpoint. The Key should be in the format projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}. See: https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt  (Example: projects/your-project-id/locations/global/keyRings/your-keyring/cryptoKeys/your-key-name).
* tokenSecretId: Secret Manager secret ID for the token. This parameter should be provided if `tokenSource` is set to SECRET_MANAGER. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}. (Example: projects/your-project-id/secrets/your-secret/versions/your-secret-version).
* rootCaCertificatePath: The full URL to root CA certificate in Cloud Storage. The certificate provided in Cloud Storage must be DER-encoded and may be supplied in binary or printable (Base64) encoding. If the certificate is provided in Base64 encoding, it must be bounded at the beginning by -----BEGIN CERTIFICATE-----, and must be bounded at the end by -----END CERTIFICATE-----. If this parameter is provided, this private CA certificate file will be fetched and added to Dataflow worker's trust store in order to verify Splunk HEC endpoint's SSL certificate which is signed by that private CA. If this parameter is not provided, the default trust store is used. (Example: gs://mybucket/mycerts/privateCA.crt).
* enableBatchLogs: Specifies if logs should be enabled for batches written to Splunk. Defaults to: true.
* enableGzipHttpCompression: Specifies if HTTP requests sent to Splunk HEC should be GZIP encoded. Defaults to: true.
* jsonSchemaPath: Path to JSON schema, ex gs://path/to/schema. Default: null.
* javascriptTextTransformGcsPath: GCS path to Javascript UDF source. UDF will be preferred option for transformation if supplied. If this parameter is supplied, a `javascriptTextTransformFunctionName` parameter will also be required. Otherwise, the UDF will not execute. Default: null
* javascriptTextTransformFunctionName: UDF Javascript Function Name. If a `javascriptTextTransformGcsPath` parameter is supplied, this parameter is required. Otherwise, the UDF will not execute. Default: null

Template can be executed using the following `gcloud` command:

```sh
export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=gs://path-to-image-spec-file \
        --parameters inputFileSpec=gs://path-to-csv-file  \
        --parameters containsHeaders=<true-or-false> \
        --parameters invalidOutputPath=gs://path-to-error-folder \
        --parameters url=<splunk-hec-url> \
        --parameters token=<splunk-token>

gcloud dataflow flex-template run "gcs-to-splunk-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location gs://path-to-image-spec-file \
  --parameters invalidOutputPath=gs://path-to-error-folder \
  --parameters inputFileSpec=gs://path-to-csv-file \
  --parameters containsHeaders=<true-or-false> \
  --parameters delimiter=<delimiter> \
  --parameters csvFormat=<csv-format> \
  --parameters url=<splunk-hec-url> \
  --parameters batchCount=<batch-count> \
  --parameters disableCertificateValidation=<true-or-false> \
  --parameters parallelism=<parallelism> \
  --parameters tokenSource=<KMS/SECRET_MANAGER/PLAINTEXT> \
  --parameters token=<splunk-token> \
  --parameters tokenKMSEncryptionKey=<token-encryption-key> \
  --parameters tokenSecretId=<token-secret-id> \
  --parameters rootCaCertificatePath=gs://path-to-cert \
  --parameters enableBatchLogs=<true-or-false> \
  --parameters enableGzipHttpCompression=<true-or-false> \
  --parameters jsonSchemaPath=gs://path/to/schema \
  --parameters javascriptTextTransformGcsPath=gs://path/to/transform \
  --parameters javascriptTextTransformFunctionName=<transform-name>
```
