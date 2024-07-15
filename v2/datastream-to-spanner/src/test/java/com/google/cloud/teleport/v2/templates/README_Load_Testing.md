# Datastream to Spanner Load Testing Data Generation
The [DataStreamToSpanner](src/main/java/com/google/cloud/teleport/v2/templates/DataStreamToSpanner.java) pipeline
ingests data supplied by DataStream, optionally applies a Javascript or Python UDF if supplied
and writes the data to Cloud Spanner database. To verify horizontal scalability, reliability, performance 
variation based on data shapes of migration dataflow templates and to identify any regression during feature
release, the dataflow template needs to load tested. These tests will benchmark measures such as throughput, data-correctness 
and time taken. These load tests will run on a weekly schedule.

Following steps will be used to generate fixed datasets to ensures all benchmarks run against the same set of 
sources, eliminating any performances changes due to differences in the source database.


### Data Generation Steps
This approach for data generation generates a csv file and imports it to a CLoud Sql Instance. All rows are identical
and primary key is generated through auto-increment.

1. Cloud SQL Instance can be created by following the following [guide](https://cloud.google.com/sql/docs/mysql/create-instance).
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

2. Create tables in the instance with Primary Key as Auto-Increment by either connecting to it through the 
[shell](https://cloud.google.com/sql/docs/mysql/connect-instance-cloud-shell) or through 
[Cloud Sql Studio](https://cloud.google.com/sql/docs/mysql/manage-data-using-studio)

3. Create a csv file with a single row with primary key omitted as we rely on MySql auto-increment for the primary-key
 generation.

4. Create GCS Bucket and upload csv file to it.
```shell
export BUCKET_NAME=<my-bucket>
export BUCKET_LOCATION=<region>
export CSV_FILE_PATH=<file-path>

gcloud storage buckets create gs://${BUCKET_NAME} --location=${BUCKET_LOCATION}
gcloud storage cp ${CSV_FILE_PATH} gs://${BUCKET_NAME}
```

5. Use [GSUTIL COMPOSE](https://cloud.google.com/storage/docs/gsutil/commands/compose) to merge these CSV files and 
generate larger files of required row counts.
```shell
export MERGE_COUNT=<number>
export BUCKET_NAME=<my-bucket>
export GCS_FILE_PATH=<my-csv-file>

for i in {1..${MERGE_COUNT}}; do
  gsutil compose gs://${BUCKET_NAME}/${FILE_NAME} gs://${BUCKET_NAME}/${FILE_NAME} gs://${BUCKET_NAME}/${FILE_NAME}
done
```

6. Import [CSV to Cloud Sql Table](https://cloud.google.com/sql/docs/mysql/import-export/import-export-csv#import_data_from_a_csv_file_to)
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
