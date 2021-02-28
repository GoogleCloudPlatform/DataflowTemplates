The [PubSubToSnowflake](src/main/java/com/mw/pipeline/stream/DataflowToSnowflakePipeline.java) pipeline is a Stream pipeline which ingests CSV format messages from Pub/Sub subscription and outputs them into a Snowflake table.

### Requirements

Java 8
Maven 3
Python 3.x - to generate base64 equivalent of a sensitive parameter value
OpenSSL to generate RSA key
Curl - to generate encrypted values for sensitive parameter using base64 equivalent and cryptographic key
Pub/Sub topic subscription
Access to Pub/Sub Topic subscription
Access to run Dataflow job on GCP
Access to create cryptographic key on GCP
Access to Snowflake table to write to

### Getting Started

This is a classic template which uses encrypted values for authentication parameters to protect sensitive info. 

The required setup is below:

### Building Template - Compiling the pipeline

Execute the following command from the directory containing the parent pom.xml (v2/pubsub-to-snowflake):

mvn clean install

mvn compile exec:java \
 -Dexec.mainClass=com.mw.pipeline.stream.DataflowToSnowflakePipeline \
	-Dexec.args=" \
	--runner=dataflow \
	--region=<region - preferably same region as Snowflake> \
--stagingLocation=gs://<bucket name>/staging  \
--templateLocation=gs://<bucket name>/templates/PubSubSubscriptiontoSnowflake"

Also, copy the provided template metadata file to template location:
gsutil cp src/main/resources/PubSubSubscriptiontoSnowflake_metadata gs://<bucket name>/templates/PubSubSubscriptiontoSnowflake_metadata

## Generating RSA Key

Generate rsa private/public key combination to use with this stream dataflow template: 
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snow_id_rsa.p8

To obtain the Public key content:
openssl rsa -in snow_id_rsa.p8 -pubout

Use the private/public key and passphrase as described below.

### Generating cryptographic key and establishing sensitive parameter value encryption

Create a key ring:
gcloud kms keyrings create sfconnector_keyring --location=<region>

Create a key for the created key ring:
gcloud kms keys create sfconnector_kms_key  --location=<region>  --keyring=sfconnector_keyring  --purpose=encryption

Cryptographic key Access For Service user: (Connect this policy to the gcp service account which would be used for executing dataflow jobs built using this stream dataflow template) 

gcloud kms keys add-iam-policy-binding sfconnector_kms_key \
    --keyring sfconnector_keyring \
    --location <region of the key ring> \
    --member serviceAccount:<ServiceUserName>@<ProjectId>.iam.gserviceaccount.com  \
    --role roles/cloudkms.cryptoKeyEncrypterDecrypter

This method described in the following article is used for the parameter encryption (https://medium.com/google-cloud/using-google-cloud-key-management-service-with-dataflow-templates-71924f0f841f)

In the list of template parameters below, three parameters (User Name, Private Key, Passphrase) are marked as sensitive. Generate base64 equivalent of the value for each of these three parameters:

Using Python 3.x:
import base64
base64.b64encode(b'<parameter plain text value e.g. value dataflow_user for username>')

Expected Output: b’<base64 value>’

Then using the base64 value via a curl command, generate the encrypted value using the cryptographic key:

curl -s -X POST "https://cloudkms.googleapis.com/v1/projects/<ProjectId>/locations/<Region>/keyRings/<KeyRing Name>/cryptoKeys/<Key Name>/cryptoKeyVersions/1:encrypt"  -d "{\"plaintext\":\"<Base64 value>\"}"  -H "Authorization:Bearer $(gcloud auth application-default print-access-token)"  -H "Content-Type:application/json"

Expected Output:
{
  "name": "projects/<ProjectId>/locations/<Region>/keyRings/<KeyRing Name>/cryptoKeys/<Key Name>/cryptoKeyVersions/1",
  "ciphertext": "<Encrypted value - to be used as the sensitive parameter value for dataflow job. It will need to be created for 3 parameters (username, private key content and passphrase)>",
  "ciphertextCrc32c": "<Crc32c value - not to be used>",
  "protectionLevel": "SOFTWARE"
}

### Setting up Snowflake Environment

Execute the following commands in the Snowflake environment. The following setup prepares the Snowflake environment to accept messages from a Pub/Sub subscription to Snowflake table. 

An example of Taxi ride data is used to describe the setup below:
use role accountadmin;
create database if not exists TAXI_DB comment = 'Database to store taxi ride stats';

create warehouse if not exists TAXI_WH with warehouse_size = XSMALL comment = 'Warehouse to process loading of taxi rides data and to query loaded data';

create role if not exists TAXI_ROLE comment = 'For use with ingest/extract/query of taxi rides data';
 
grant role TAXI_ROLE to user <current user - user creating the snowflake objects>;
grant usage on database TAXI_DB to TAXI_ROLE;
grant usage on warehouse TAXI_WH to TAXI_ROLE;
           
-- Setup Snowflake user
create user if not exists  dataflow_user password= '<set user password>' login_name=dataflow_user
default_warehouse = TAXI_WH 
default_namespace = TAXI_DB;

grant role TAXI_ROLE to user DATAFLOW_USER;
alter user DATAFLOW_USER set default_role = TAXI_ROLE;
alter user DATAFLOW_USER set rsa_public_key='<enter encrypted public key for the passphrase here>';

-- Create database schema for table 
use database TAXI_DB;
create schema if not exists RIDES_SH comment = 'Database schema to host tables for taxi ride data';

use schema RIDES_SH;

create or replace table RIDES_SH.TRIP_STREAM_WRITE
( unique_key string,
  taxi_id string,
  trip_start_timestamp string,
  trip_end_timestamp string
);


create storage integration if not exists gcs_int_dataflow 
	  type = external_stage
	  storage_provider = gcs
	  enabled = true
	  storage_allowed_locations = ('gcs://<bucket name>');
   -- Here ensure that location is entered as 'gcs://<bucket name>' and not as 'gcs://<bucket name>/'

desc storage integration gcs_int_dataflow;

/*
Here retrieve the value for column “property_value” where column “property” value = “STORAGE_GCP_SERVICE_ACCOUNT”. The returned value is the Snowflake GCP Service user. Grant this GCP service user a read/write access to the storage bucket which will be used for temporary data processing before the data gets loaded to Snowflake. The value would be formatted as (<Snowflake assigned name>@gcp<region>-<zone>.iam.gserviceaccount.com)
*/


CREATE OR REPLACE FILE FORMAT RIDES_FILE_FORMAT
type = CSV
comment = 'File format for taxi stats data';

     /* Ensure that GCP service account for Snowflake has read/write access to location specified in the URL below */

CREATE OR REPLACE STAGE  DATAFLOW_STAGE
  URL = 'gcs://<bucket name>/sf_processing/stream_load'
  STORAGE_INTEGRATION = gcs_int_dataflow 
  file_format = ( FORMAT_NAME = TAXI_DB.RIDES_SH.RIDES_FILE_FORMAT, type = CSV)
  copy_options = ( ON_ERROR = CONTINUE )
  comment = 'Stage for ingesting data';

-- Transfer ownership of created objects to role TAXI_ROLE

      grant ownership on file format RIDES_FILE_FORMAT to TAXI_ROLE;
      grant ownership on stage DATAFLOW_STAGE to TAXI_ROLE;
      grant ownership on integration gcs_int_dataflow to TAXI_ROLE;
      grant ownership on table trip_stream_write to TAXI_ROLE;
      grant ownership on schema RIDES_SH to TAXI_ROLE;

-- Create snowpipe object to be used to load contents of Stream data to snowflake table
use role TAXI_ROLE;


CREATE OR REPLACE PIPE DATAFLOW_SNOWPIPE_GCP
	  as
	  copy into RIDES_SH.TRIP_STREAM_WRITE
	  from @DATAFLOW_STAGE;

### Setting up Snowflake service user access to GCS bucket

gsutil iam ch  "serviceAccount":<name>@<ProjectId>.iam.gserviceaccount.com:roles/storage.legacyBucketWriter gs://<Bucket Name>

### Setting up access for a GCP service user to run Dataflow jobs

gcloud projects add-iam-policy-binding <Project Id> \   --member='serviceAccount:<ServiceUserName>@<ProjectId>.iam.gserviceaccount.com' --role='roles/dataflow.developer'

### Executing Template
The template requires the following parameters:

1. Snowflake Server Name
Name of the Snowflake server e.g. <Snowflake Account>.<Snowflake region>.gcp.Snowflakecomputing.com

2. KMS Encryption Key
KMS Encryption Key (format: projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}). 

3. Snowflake User
(Sensitive Parameter)
Encrypted form of Snowflake service user name e.g. user name “dataflow_user” in encrypted form. This user would have the public key and the needed Snowflake custom role  assigned to it e.g. TAXI_ROLE. The Snowflake custom role referred here also gets passed as a parameter (shown below)

4. Raw Private Key
(Sensitive Parameter)
Encrypted form of the raw content (only) from the private key. It is needed for authentication to Snowflake

5. Passphrase for the private key
(Sensitive Parameter)
Encrypted form of the Passphrase for the private key. It is needed for authentication to Snowflake

6. Snowflake’s Storage Integration Name
Name of the created Snowflake’s storage integration object. The storage integration enables reading/writing on defined bucket for intermediate data processing e.g. gcs_int_dataflow

7. Snowflake’s Snowpipe Name
Name of the defined Snowflake’s snowpipe object. The snowpipe object loads the prepared micro-batch from the source stream data to the Snowflake table e.g. DATAFLOW_SNOWPIPE_GCP

8. Snowflake’s Role
Defined custom Snowflake’s role. e.g. TAXI_ROLE

9. Snowflake’s Warehouse Name
Name of Snowflake’s “warehouse” component to be used for data export processing e.g. TAXI_WH

10. Snowflake’s Database Name
Name of Snowflake’s “database component e.g. TAXI_DB

11. Snowflake’s Schema Name
Name of the Snowflake’s database “schema” hosting target table e.g. RIDES_SH

12. Snowflake’s Table Name
The name of Snowflake’s target table where the streaming data will load e.g. TRIP_STREAM_WRITE

13. Pub Sub Topic Subscription
Name of Pub Sub Topic subscription to obtain stream data e.g. projects/<project>/subscriptions/<subscription name>

14. Staging Bucket
Name of the Cloud Storage bucket location where micro-batches would be built from streaming data before these get loaded into Snowflake. This location must match the URL for the snowflake’s stage object. Enter the bucket location starting with “gs://” 

15. Temporary Location
Cloud Storage bucket location for the dataflow job to be able to create any necessary temporary files for processing the data. Enter the bucket location starting with “gs://”


The Template can be executed either via the Dataflow console or  gcloud sdk. The following gcloud command can be used to run template:

gcloud dataflow jobs run <dataflow job> --gcs-location gs://<bucket>/templates/PubSubSubscriptiontoSnowflake --region <region> --staging-location gs://<bucket>/<folder>/ --service-account-email=<service account>@<GCP project>.iam.gserviceaccount.com --parameters username=<encrypted user name>,role=<snowflake role e.g.TAXI_ROLE> ,database=<snowflake database e.g. TAXI_DB>,schema=<snowflake schema e.g. RIDES_SH>,warehouse=<Snowflake Warehouse e.g. TAXI_WH>,stagingBucketName=<Staging bucket location>,serverName=<Snowflake server name e.g. <snowflake account>.<region>.gcp.snowflakecomputing.com,inputSubscription=<pub/Sub subscription e.g. projects/<project>/subscriptions/<subscription name>,table=<snowflake table name>,snowPipe=<snowpipe name e.g. DATAFLOW_SNOWPIPE_GCP>,storageIntegrationName=<Snowflake storage integration name e.g. gcs_int_dataflow>,KMSEncryptionKey=<KMS key e.g. projects/<project name>/locations/<region>/keyRings/<keyring name>/cryptoKeys/<key name>,rawPrivateKey="<encrypted contents of private key>",privateKeyPassphrase="<Encrypted passphrase>"