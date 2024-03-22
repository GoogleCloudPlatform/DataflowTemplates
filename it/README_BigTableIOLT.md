# How to run BigTableIOLT

[BigTableIOLT](google-cloud-platform/src/test/java/com/google/cloud/teleport/it/gcp/bigtable/BigTableIOLT.java) 
is a load test for [BigTableIO](https://github.com/apache/beam/blob/master/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/bigtable/BigtableIO.java).

## Prerequisites
* Java 11
* Maven
* gcloud CLI, and execution of the following commands:
  * gcloud auth login
  * gcloud auth application-default login
  * GCP project
  * GCP bucket
  * BigTable service should be enabled in GCP

## Run test using Maven

You can run all tests using this command:

```
mvn test -pl it/google-cloud-platform -am
    -Dtest=BigTableIOLT \
    -Dproject=[gcpProject] \
    -DartifactBucket=[temp bucket] \
    -DfailIfNoTests=false \
    -Dconfiguration=local
```

You can run specific test using this command:

```
mvn test -pl it/google-cloud-platform -am \
    -Dtest="BigTableIOLT#testWriteAndRead" \
    -Dproject=[gcpProject] \
    -DartifactBucket=[temp bucket] \
    -DfailIfNoTests=false \
    -Dconfiguration=local
```

### Configuration property

There are 4 options for configuration property:
* local - it will be run on the DirectRunner with 1.000 rows which is approximately 1 MB.
* small - it will be run on the DataflowRunner with 1.000.000 rows which is approximately 1 GB.
* medium - it will be run on the DataflowRunner with 10.000.000 rows which is approximately 10 GB.
* large - it will be run on the DataflowRunner with 100.000.000 rows which is approximately 100 GB.
