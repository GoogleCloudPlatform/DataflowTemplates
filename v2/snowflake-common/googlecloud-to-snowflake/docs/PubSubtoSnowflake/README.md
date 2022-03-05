# Pub/Sub to Snowflake Dataflow Template

The PubSubToSnowflake pipeline is a streaming pipeline which ingests data from Pub/Sub subscription and outputs the messages into Snowflake. The template code base is available under (DataflowTemplates/v2/GoogleCloud-to-Snowflake).

## Getting Started
### Requirements

* Java 8
* Maven
* Python 3.x - to generate base64 equivalent of a sensitive parameter value
* OpenSSL to generate RSA key
* curl - to generate encrypted values for sensitive parameter using base64 equivalent and cryptographic key
* Pub/Sub
* Snowflake environment and write access to target table
* Access to Pub/Sub Topic subscription
* Access to run Dataflow job on GCP
* Access to create cryptographic key on GCP


### Building Template

This is a flex template which provides a flexibility to use raw or encrypted values for authentication parameters. 

The supported source data formats are:
1. CSV
2. NEWLINE DELIMITED JSON (non-nested only),

The required setup is below:

Initially generate an RSA Key to use the key pair based credentials. The template uses a connector for snowflake which is part of the Apache Beam (v2.28). Due to this dependency, the stream data load template currently supports only key pair based authentication.

All sensitive parameters can be encrypted using KMS (Optional) if desired. To supply encrypted value for any parameter, embed encrypted value in templated format i.e ‘${<encryptedvalue>}’.

Refer to [KMS documentation](https://cloud.google.com/kms/docs/encrypt-decrypt#kms-encrypt-symmetric-api) to learn how to encrypt the value. Following details provides an overview of generating the encrypted value of a parameter.

### Generating cryptographic key and encrypt sensitive parameter values
    
Create a key ring:
```
gcloud kms keyrings create <keyring-name> \
    --location=<region>
```
    
Create a key for the created key-ring:
```   
gcloud kms keys create <key-name> \
    --location=<region> \
    --keyring=<keyring-name> \
    --purpose=encryption
```


Cryptographic key Access For Service user: (Connect this policy to the gcp service account which would be used for executing dataflow jobs built using this stream dataflow template) 
```
gcloud kms keys add-iam-policy-binding <key-name> \
    --keyring <keyring-name> \
    --location <region of the key-ring> \
    --member serviceAccount:<ServiceUserName>@<ProjectId>.iam.gserviceaccount.com \
    --role roles/cloudkms.cryptoKeyEncrypterDecrypter
```
Refer to [Snowflake documentation](https://docs.snowflake.com/en/) to learn how to set up Snowflake components referred to in the template parameters.
Compiling the pipeline
Execute the following steps from the directory containing the parent pom.xml (v2/):

Setup the environment variable values:

```
export PROJECT=project-id
export IMAGE_NAME=pubsub_to_snowflake
export BUCKET_NAME=gs://Bucket Name
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE=pubsub-to-snowflake
export APP_ROOT=/template/${TEMPLATE_MODULE}
export COMMAND_SPEC=${APP_ROOT}/resources/${TEMPLATE_MODULE}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/templates/${IMAGE_NAME}-image-spec.json
export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S-%N`"

```
Build and push image to Google Container Repository

```
mvn clean package -Dimage=${TARGET_GCR_IMAGE} \
                  -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
                  -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
                  -Dapp-root=${APP_ROOT} \
                  -Dcommand-spec=${COMMAND_SPEC} \
                  -pl ${TEMPLATE_MODULE} -am
```
Create file in Cloud Storage with path to container image in Google Container Repository and the metadata information explaining various parameters.

Note: The image property would point to the ${TARGET_GCR_IMAGE} defined previously.

```
{
  "image": "gcr.io/project-id/image-name",
  "metadata": {
    "name": "PubSubSubscriptiontoSnowflake",
    "description": "PubSub to Snowflake",
    "parameters": [
          {
           "name": "serverName",
            "helpText": "Snowflake Server Name",
            "label": "Enter Snowflake Server Name ",
            "paramType": "TEXT",
            "isOptional": false
          },
          {
            "name": "tokenKMSEncryptionKey",
            "helpText": "KMS Encryption Key",
            "label": "Enter KMS Encryption Key (format: projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name})",
            "paramType": "TEXT",
            "isOptional": true
          },
          {
            "name": "username",
            "helpText": "Snowflake User Name",
            "label": "Enter Snowflake User Name ",
            "paramType": "TEXT",
            "isOptional": false
          },
          {
            "name": "rawPrivateKey",
            "helpText": "Raw private key",
            "label": "Enter raw private key string",
            "paramType": "TEXT",
            "isOptional": false
          },
          {
            "name": "privateKeyPassphrase",
            "helpText": "Passphrase for the private key",
            "label": "Enter Passphrase for the private key",
            "paramType": "TEXT",
            "isOptional": false
          },
          {
            "name": "storageIntegrationName",
            "helpText": "Snowflake's storage integration name",
            "label": "Enter Snowflake's storage integration name",
            "paramType": "TEXT",
            "isOptional": false
          },
          {
            "regexes": [
              "^[^\\n\\r]+$"
            ],
            "name": "snowPipe",
            "helpText": "Snowflake's snowPipe name",
            "label": "Snowflake's snowPipe name",
            "paramType": "TEXT",
            "isOptional": false
          },
          {
            "name": "role",
            "helpText": "Snowflake role",
            "label": "Enter Snowflake role",
            "paramType": "TEXT",
            "isOptional": false
          },
          {
            "name": "warehouse",
            "helpText": "Snowflake's warehouse name",
            "label": "Enter Snowflake's warehouse name",
            "paramType": "TEXT",
            "isOptional": false
          },
          {
            "name": "database",
            "helpText": "Snowflake's database name",
            "label": "Enter Snowflake's database name",
            "paramType": "TEXT",
            "isOptional": false
          },
          {
            "name": "schema",
            "helpText": "Snowflake's database schema name",
            "label": "Enter Snowflake's database schema name",
            "paramType": "TEXT",
            "isOptional": false
          },
          {
            "name": "table",
            "helpText": "Snowflake's table name",
            "label": "Enter Snowflake's table name",
            "paramType": "TEXT",
            "isOptional": false
          },
          {
            "name": "inputSubscription",
            "helpText": "Name of the Pub/Sub Subscription",
            "label": "Enter name of the pub/sub subscription",
            "paramType": "PUBSUB_SUBSCRIPTION",
            "isOptional": false
          },
          {
            "name": "stagingBucketName",
            "helpText": "Staging bucket location starting with (gs://) for data processing micro-batches",
            "label": "Enter staging bucket location starting with (gs://) for data processing micro-batches",
            "paramType": "GCS_READ_FILE",
            "isOptional": false
          },
          {
            "name": "oauthToken",
            "helpText": "OAuth token",
            "label": "Enter OAuth token to set OAuth authentication.",
            "paramType": "TEXT",
            "isOptional": true
          },
          {
            "name": "password",
            "helpText": "Password",
            "label": "Enter Password to set username/password authentication.",
            "paramType": "TEXT",
            "isOptional": true
          },
          {
            "name": "gcpTempLocation",
            "helpText": "Temporary location in GCP",
            "label": "Enter Temporary location in GCP.",
            "paramType": "GCS_READ_FILE",
            "isOptional": true
          },
          {
            "name": "sourceFormat",
            "helpText": "Source format of pubsub data",
            "label": "Enter source format of pubsub data.",
            "paramType": "TEXT",
            "isOptional": false
          }
    ]
  },
  "sdkInfo": {
    "language": "JAVA"
  }
}
```

### Executing Template
The template requires the following parameters. If needed, the parameters marked with ”*” can be sent in encrypted form by specifying the parameter value in the format of (${<encrypted form of parameter value>}).

   1. Source Format*:
      Format of the source data. The template currently supports the following source format values CSV, JSON (non-nested, complete JSON record in single line or non-nested). The default is ‘CSV’. Optionally AVRO (not-nested, complete JSON record in single line) with AVRO schema defined at the Pub/Sub Topic.
   2. Snowflake Server Name*:
      Name of the Snowflake server e.g. <Snowflake Account>.<Snowflake region>.gcp.Snowflakecomputing.com
   3. KMS Encryption Key:
      KMS Encryption Key (format: projects/{gcp_project}/locations/{key_region}/keyRings/{keyring-name}/cryptoKeys/{key-name}). 
   4. Snowflake User*:
      Snowflake service user name e.g. user name “dataflow_user”. This user would have the public key and the needed Snowflake custom role  assigned to it e.g. TAXI_ROLE. The Snowflake custom role referred here also gets passed as a parameter (shown below)
   5. Raw Private Key*:
      The raw content (only) from the private key. It is needed for authentication to Snowflake.
   6. Passphrase for the private key*:
      Passphrase for the private key. It is needed for authentication to Snowflake
   7. Snowflake’s Storage Integration Name*:
      Name of the created Snowflake’s storage integration object. The storage integration enables reading/writing on defined bucket for intermediate data processing e.g. gcs_int_dataflow
   8. Snowflake’s Snowpipe Name*:
      Name of the defined Snowflake’s snowpipe object. The snowpipe object loads the prepared micro-batch from the source stream data to the Snowflake table e.g. DATAFLOW_SNOWPIPE_GCP
   9. Snowflake’s Role*:
      Defined custom Snowflake’s role. e.g. TAXI_ROLE
  10. Snowflake’s Warehouse Name*:
      Name of Snowflake’s “warehouse” component to be used for data export processing e.g. TAXI_WH
  11. Snowflake’s Database Name*:
      Name of Snowflake’s “database component e.g. TAXI_DB
  12. Snowflake’s Schema Name*:
      Name of the Snowflake’s database “schema” hosting target table e.g. RIDES_SH
  13. Snowflake’s Table Name*:
      The name of Snowflake’s target table where the streaming data will load e.g. TRIP_STREAM_WRITE
  14. Pub Sub Topic Subscription*:
      Name of Pub Sub Topic subscription to obtain stream data e.g. projects/<project>/subscriptions/<subscription name>
  15. Staging Bucket*:
      Name of the Cloud Storage bucket location where micro-batches would be built from streaming data before these get loaded into Snowflake. This location must match the URL for the snowflake’s stage object. Enter the bucket location starting with “gs://” 

The Template can be executed either via the Dataflow console or  gcloud sdk. 

The following gcloud command can be used to run the flex template:

```
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} \
        --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --staging-location gs://<bucket>/<folder>/ \
        --service-account-email=<service account>@<GCP project>.iam.gserviceaccount.com \
        --parameters \
             username=<user name>, \
             role=<snowflake role e.g.TAXI_ROLE>, \
             database=<snowflake database e.g. TAXI_DB>, \
             schema=<snowflake schema e.g. RIDES_SH>, \
             warehouse=<Snowflake Warehouse e.g. TAXI_WH>, \
             stagingBucketName=<Staging bucket location>, \
             serverName=<Snowflake server name e.g. <snowflake account>.<region>.gcp.snowflakecomputing.com, \
             inputSubscription=<pub/Sub subscription e.g. projects/<project>/subscriptions/<subscription name>, \
             table=<snowflake table name>, \
             snowPipe=<snowpipe name e.g. DATAFLOW_SNOWPIPE_GCP>, \
             storageIntegrationName=<Snowflake storage integration name e.g. gcs_int_dataflow>, \
             tokenKMSEncryptionKey=<KMS key e.g. projects/<project name>/locations/<region>/keyRings/<keyring name>/cryptoKeys/<key name>, \
             rawPrivateKey='<Raw Private Key contents>', \
             privateKeyPassphrase=<passphrase>, \
             sourceFormat=<source data format e.g. csv>
```
    
