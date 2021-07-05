# Pub/Sub to Snowflake Dataflow Template

The PubSubToSnowflake pipeline is a Stream pipeline which ingests data from Pub/Sub subscription and outputs the messages into Snowflake. The template code base is available under (DataflowTemplates/v2/GoogleCloud-to-Snowflake).

**Requirements**

* Java 8
* Maven
* Python 3.x - to generate base64 equivalent of a sensitive parameter value
* OpenSSL to generate RSA key
* Curl - to generate encrypted values for sensitive parameter using base64 equivalent and cryptographic key
* Pub/Sub
* Snowflake environment and write access to target table
* Access to Pub/Sub Topic subscription
* Access to run Dataflow job on GCP
* Access to create cryptographic key on GCP

**Getting Started**
**Building Template**

This is a flex template which provides a flexibility to use raw or encrypted values for authentication parameters. The support source data formats are CSV, JSON (not-nested, complete JSON record in single line), and optionally AVRO (not-nested, complete JSON record in single line) with AVRO schema defined at the Pub/Sub Topic.
The required setup is below:
Initially generate an RSA Key to use the key pair based credentials. The template uses a connector for snowflake which is part of the Apache Beam (v2.28). Due to this dependency, the stream data load template currently supports only key pair based authentication.

All sensitive parameters can be encrypted using KMS (Optional) if desired. To supply encrypted value for any parameter, embed encrypted value in templated format i.e ‘${<encryptedvalue>}’.

Refer to [KMS documentation](https://cloud.google.com/kms/docs/encrypt-decrypt#kms-encrypt-symmetric-api) to learn how to encrypt the value. The following provides an overview of generating the encrypted value of a parameter.

**Generating cryptographic key and encrypt sensitive parameter values**
    
Create a key ring:
```
gcloud kms keyrings create <keyring-name> --location=<region>
```
    
Create a key for the created key-ring:
```   
gcloud kms keys create <key-name>  --location=<region>  --keyring=<keyring-name>  --purpose=encryption
```
    
Cryptographic key Access For Service user: (Connect this policy to the gcp service account which would be used for executing dataflow jobs built using this stream dataflow template) 
```
gcloud kms keys add-iam-policy-binding <key-name> \
    --keyring <keyring-name>  \
    --location <region of the key-ring> \
    --member serviceAccount:<ServiceUserName>@<ProjectId>.iam.gserviceaccount.com  \
    --role roles/cloudkms.cryptoKeyEncrypterDecrypter
```
Refer to [Snowflake documentation](https://docs.snowflake.com/en/) to learn how to set up Snowflake components referred to in the template parameters.
Compiling the pipeline
Execute the following command from the directory containing the parent pom.xml (v2/):
```
mvn clean install

gcloud dataflow flex-template build \ gs://<bucket Name>/templates/PubSubToSnowflake_flex \ 
--image-gcr-path gcr.io/<GCP Project ID>/pubsub_to_snowflake:latest \ 
--sdk-language JAVA \ 
--flex-template-base-image JAVA8 \ 
--jar target/googlecloud-to-snowflake-1.0-SNAPSHOT.jar \
--metadata-file src/main/resources/PubSubToSnowflake_metadata.json \
--env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.google.cloud.teleport.v2.snowflake.templates.PubSubToSnowflake"
```
    
Executing Template
The template requires the following parameters. If needed, the parameters marked with ”*” can be sent in encrypted form by specifying the parameter value in the format of (${<encrypted form of parameter value>}).



The Template can be executed either via the Dataflow console or  gcloud sdk. The following gcloud command can be used to run template:

```
gcloud dataflow flex-template run <dataflow job> --template-file-gcs-location gs://<bucket>/templates/PubSubToSnowflake_flex --region <region> --staging-location gs://<bucket>/<folder>/ --service-account-email=<service account>@<GCP project>.iam.gserviceaccount.com --parameters username=<user name>,role=<snowflake role e.g.TAXI_ROLE>,database=<snowflake database e.g. TAXI_DB>,schema=<snowflake schema e.g. RIDES_SH>,warehouse=<Snowflake Warehouse e.g. TAXI_WH>,stagingBucketName=<Staging bucket location>,serverName=<Snowflake server name e.g. <snowflake account>.<region>.gcp.snowflakecomputing.com,inputSubscription=<pub/Sub subscription e.g. projects/<project>/subscriptions/<subscription name>,table=<snowflake table name>,snowPipe=<snowpipe name e.g. DATAFLOW_SNOWPIPE_GCP>,storageIntegrationName=<Snowflake storage integration name e.g. gcs_int_dataflow>,tokenKMSEncryptionKey=<KMS key e.g. projects/<project name>/locations/<region>/keyRings/<keyring name>/cryptoKeys/<key name>,rawPrivateKey='<Raw Private Key contents>',privateKeyPassphrase=<Encrypted passphrase>,sourceFormat=<source data format e.g. csv>
```
    
