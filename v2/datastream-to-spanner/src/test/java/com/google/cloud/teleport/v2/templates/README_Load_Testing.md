# Datastream to Spanner Load Testing Data Generation
The [DataStreamToSpanner](src/main/java/com/google/cloud/teleport/v2/templates/DataStreamToSpanner.java) pipeline
ingests data supplied by DataStream, optionally applies a Javascript or Python UDF if supplied
and writes the data to Cloud Spanner database. To verify horizontal scalability, reliability, performance 
variation based on data shapes of migration dataflow templates and to identify any regression during feature
release, the dataflow template needs to load tested. These tests will benchmark measures such as throughput, data-correctness 
and time taken.

Following steps will be used to generate fixed datasets to ensures all benchmarks run against the same set of 
sources, eliminating any performances changes due to differences in the source database.


### Create Cloud SQL Instance
Cloud SQL Instance can be created by following the following [guide](https://cloud.google.com/sql/docs/mysql/create-instance).
```shell
export INSTANCE_NAME=<my-instance>
export NUMBER_CPUS=<number-of-cpus>
export MEMORY_SIZE=<memory-size>
export REGION=<region>

gcloud sql instances create ${INSTANCE_NAME} \
--cpu=${NUMBER_CPUS} \
--memory=${MEMORY_SIZE} \
--region=${REGION} \
--database-version=MYSQL_8_0
```

### Data Generation using Dataflow Streaming Data Generator for JDBC
1. Follow the [guide](https://cloud.google.com/vpc/docs/create-modify-vpc-networks) to create a VPC Network with the following
   properties:
    1. Create new subnet with `Private Google Access` as On
    2. Set `Private Google Access` as **Global**

```shell
export PROJECT_NAME=<my-project>
export NETWORK_NAME=<my-network>
export SUBNET_NAME=<my-subnet>

gcloud compute networks create ${NETWORK_NAME} --project=${PROJECT_NAME} \
 --subnet-mode=custom --mtu=1460 --bgp-routing-mode=global 
 
gcloud compute networks subnets create ${SUBNET_NAME} --project=${PROJECT_NAME} \
--range=10.0.0.0/24 --stack-type=IPV4_ONLY --network=${NETWORK_NAME} --region=us-central1 \
--enable-private-ip-google-access
```

2. Modify SQL Instance and configure the VPC network created in the previous step to the Cloud SQL Instance by
   following this [guide](https://cloud.google.com/sql/docs/mysql/configure-private-ip?_gl=1*ocwud2*_ga*MTI0MzA5MzM4My4xNzE4OTQ3NzU1*_ga_WH2QY8WWF5*MTcyMTIzOTUxNi42LjEuMTcyMTI0MzgzMC4zLjAuMA..#existing-private-instance)
```shell
export CLOUD_SQL_INSTANCE_ID=<my-instance>
export PROJECT_NAME=<my-project>
export NETWORK_PROJECT_ID=<my-network-project>
export NETWORK_NAME=<my-network>

gcloud beta sql instances patch ${CLOUD_SQL_INSTANCE_ID} \
--project=${PROJECT_ID} \
--network=projects/${NETWORK_PROJECT_ID}/global/networks/${NETWORK_NAME} \
--no-assign-ip \
--enable-google-private-path
```

3. Create tables in the instance by either connecting to it through the
   [shell](https://cloud.google.com/sql/docs/mysql/connect-instance-cloud-shell) or through
   [Cloud Sql Studio](https://cloud.google.com/sql/docs/mysql/manage-data-using-studio).

   [**Sample MySQL Schema**](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/datastream-to-spanner/src/test/resources/DataStreamToSpannerLoadTesting/mysql-schema.sql)

4. Create a schema file that contains a JSON template for the generated data. For more information, see the
   [json-data-generator documentation](https://github.com/vincentrussell/json-data-generator/blob/master/README.md). Sample
   can be found [here](https://github.com/vincentrussell/json-data-generator?tab=readme-ov-file#example). Upload the schema file
   to a Cloud Storage bucket.

   [**Sample Schema File**](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/datastream-to-spanner/src/test/resources/DataStreamToSpannerLoadTesting/datagenerator-schema.json)

5. Run the template by following this [guide](https://cloud.google.com/dataflow/docs/guides/templates/provided/streaming-data-generator)
```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=<region>
export TEMPLATE_SPEC_GCSPATH="gs://dataflow-templates-${REGION}/latest/flex/Streaming_Data_Generator"
export QPS=<qps>
export MESSAGE_LIMIT=<row-count>
// Cloud Storage path of schema location
export SCHEMA_LOCATION=<schemaLocation>
// JDBC driver class name to use. Example: com.mysql.jdbc.Driver
export DRIVER_CLASS_NAME=<driverClassName>
// Url connection string to connect to the JDBC source. Example: jdbc:mysql://some-host:3306/sampledb.
export CONNECTION_URL=<connectionUrl>
// User name to be used for the JDBC connection
export USERNAME=<username>
//password : Password to be used for the JDBC connection
export PASSWORD=<password>
// statement : SQL statement which will be executed to write to the database
// The statement must specify the column names of the table in any order
// Only the values of the specified column names will be read from the json and added to the statement. 
// Example: INSERT INTO tableName (column1, column2) VALUES (?,?)
export STATEMENT=<statement>
export NETWORK_NAME=<my-network>
export SUBNETWORK=<my-subnetwork>

gcloud dataflow flex-template run streaming-data-generator-job --project ${PROJECT}   \
--region ${REGION}   --template-file-gcs-location $TEMPLATE_SPEC_GCSPATH  \
--parameters ^~^qps=${QPS}~messagesLimit=${MESSAGE_LIMIT}~schemaLocation=${SCHEMA_LOCATION}~sinkType=JDBC~driverClassName=${DRIVER_CLASS_NAME}~connectionUrl=${CONNECTION_URL}~username=${USERNAME}~password=${PASSWORD}~statement="${STATEMENT}"~network=${NETWORK_NAME}~subnetwork=${SUBNETWORK}
```

### Data Generation using CSV Import
This approach for data generation generates a csv file and imports it to a CLoud Sql Instance. All rows are identical
and primary key is generated through auto-increment.
1. Create tables in the instance with Primary Key as Auto-Increment by either connecting to it through the 
[shell](https://cloud.google.com/sql/docs/mysql/connect-instance-cloud-shell) or through 
[Cloud Sql Studio](https://cloud.google.com/sql/docs/mysql/manage-data-using-studio). The auto-increment approach is recommended
for generating large datasets by appending datasets as manual generation takes a long time.

   [**Sample MySQL Schema**](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/datastream-to-spanner/src/test/resources/DataStreamToSpannerLoadTesting/mysql-schema.sql)

2. Create a csv file with a single row with primary key omitted as we rely on MySql auto-increment for the primary-key
 generation.

3. Create GCS Bucket and upload csv file to it.
```shell
export BUCKET_NAME=<my-bucket>
export BUCKET_LOCATION=<region>
export CSV_FILE_PATH=<file-path>

gcloud storage buckets create gs://${BUCKET_NAME} --location=${BUCKET_LOCATION}
gcloud storage cp ${CSV_FILE_PATH} gs://${BUCKET_NAME}
```

4. Use [GSUTIL COMPOSE](https://cloud.google.com/storage/docs/gsutil/commands/compose) to merge these CSV files and 
generate larger files of required row counts.
```shell
export MERGE_COUNT=<number>
export BUCKET_NAME=<my-bucket>
export GCS_FILE_PATH=<my-csv-file>

for i in {1..${MERGE_COUNT}}; do
  gsutil compose gs://${BUCKET_NAME}/${FILE_NAME} gs://${BUCKET_NAME}/${FILE_NAME} gs://${BUCKET_NAME}/${FILE_NAME}
done
```

5. Import [CSV to Cloud Sql Table](https://cloud.google.com/sql/docs/mysql/import-export/import-export-csv#import_data_from_a_csv_file_to)
```shell
export INSTANCE_NAME=<my-instance>
export DATABASE_NAME=<my-database>
export TABLE_NAME=<my-table>
export BUCKET_NAME=<my-bucket>
export GCS_FILE_PATH=<my-csv-file>

gcloud sql import csv ${INSTANCE_NAME} gs://${BUCKET_NAME}/${FILE_NAME} \
--database=${DATABASE_NAME} \
--table=${TABLE_NAME}
```
