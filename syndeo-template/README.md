# Syndeo Template

The code in this directory defines a 'meta-template' (i.e. a template that can be parameterized with sources, sinks,
and intermediate transforms).

The prototype comes with a few utilties to bootstrap a simple workflow, though these are meant only for testing of the
template capabilities.

**This is a prototype and may change significantly**. It is not supported for direct use.

## How does it work?

The template works by relying on Beam transforms that implement the `SchemaTransform` interface and exist in the
template's classpath.

The template receives a *pipeline spec*, which is a specification defining the pipeline's source, sink, and intermediate
transforms. Then, the template will take the pipeline spec, and

The simplest way to configure and launch a template is using the `jsonSpecPayload` parameter, which expects a
JSON payload with the following shape:

```
{
  "source": {
    "urn": "beam:source:urn",
    "configurationParameters": {
      "host": "https://somehost",
      "port": 12345
    }
  },
  "sink": {
    "urn": "beam:sink:urn",
    "configurationParameters": {
      "projectId": "sample-project",
      "tableId": "table-for-something"
    }
  }
}
```

The easiest way to find supported Schema Transform implementations, their URNs and their configuration
parameters is by running the `GenerateConfiguration` script, developed in [PR 543](https://github.com/GoogleCloudPlatform/DataflowTemplates/pull/543):

```shell
mvn compile exec:java -pl syndeo-template/pom.xml \
       -Dexec.mainClass="com.google.cloud.syndeo.GenerateConfiguration" \
       -Dexec.args="output_file_name.prototext"
```

## Common workflow tasks

### Format code

To apply spotless formatting rules to the Syndeo template code, run the following command:

```shell
mvn -B spotless:apply compile -f pom.xml -pl syndeo-template/pom.xml
```

### Build `SchemaTransform` configurations

Users and consumers of the Syndeo Template can generate a proto text description of the supported `SchemaTransform`
implementations, as well as their requirements and parameters. To generate this configuration, see:

```shell
mvn compile exec:java -pl syndeo-template/pom.xml \
       -Dexec.mainClass="com.google.cloud.syndeo.GenerateConfiguration" \
       -Dexec.args="output_file_name.prototext"
```

Supported `SchemaTransform` and configuration parameters are configured in `SyndeoTemplate.java`. Specifically, the
`SUPPORTED_URNS` constant.

### Run tests (unit and integration tests)

Note that syndeo depends of the teleport integration testing framework, so make sure to install that locally:

```
mvn install -DskipTests -f pom.xml -pl .,it/pom.xml
```

To **run unit tests** for the Syndeo template, run the following command. Note that this command knows to skip
integration tests and only runs unit tests:

```shell
mvn clean package test -f pom.xml -pl syndeo-template/pom.xml
```

To set up your Google Cloud project for the integration tests, the following steps assume you have installed and setup [gcloud](https://cloud.google.com/sdk/gcloud).

1. Set your default project.

```shell
gcloud config set project PROJECT
```

**For more detailed information on individual tests, check out the `TESTING.md` file in this directory.**

### Push template to GCP

The following command will take the already-built artifacts and push them to GCS and GCR, where they can be utilized to run a template.

(Note: The project does not currently have an end-to-end test to validate template-based runs because the template would run as part of
syndeo, which involves repeated runs and so on).

```shell
mvn package -DskipTests -f pom.xml -pl syndeo-template/pom.xml

# Push Template to GCP:
gcloud dataflow flex-template build gs://$GCS_BUCKET_NAME/syndeo-template.json  \
    --metadata-file syndeo-template/metadata.json   --sdk-language "JAVA"  \
    --flex-template-base-image JAVA11     --image-gcr-path=gcr.io/$GCP_PROJECT/syndeo-template:latest \
    --jar "syndeo-template/target/syndeo-template-1.0-SNAPSHOT.jar"  \
    --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.google.cloud.syndeo.SyndeoTemplate"
```

### Launch a job from a staged template

After stacing a template in `gs://$GCS_BUCKET_NAME/syndeo-template.json`, you can then use that template to launch a Dataflow job.

Using the [Google API Explorer on the Flex Template Launch API](https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.flexTemplates/launch?apix_params=%7B%22projectId%22%3A%22cloud-teleport-testing%22%2C%22location%22%3A%22us-central1%22%2C%22resource%22%3A%7B%22launchParameter%22%3A%7B%22jobName%22%3A%22jobname124385%22%2C%22containerSpecGcsPath%22%3A%22gs%3A%2F%2Fbeam-testing-templates%2Fsyndeo-template.json%22%2C%22parameters%22%3A%7B%22jsonSpecPayload%22%3A%22%7B%20%5C%22source%5C%22%3A%20%7B%20%20%20%5C%22urn%5C%22%3A%20%5C%22bigquery%3Aread%5C%22%2C%20%20%20%5C%22configurationParameters%5C%22%20%3A%20%7B%20%20%20%20%20%5C%22table%5C%22%3A%20%5C%22dataflow-syndeo.taxirides.realtime%5C%22%20%20%20%7D%20%20%20%7D%2C%20%5C%22sink%5C%22%3A%20%7B%20%20%20%5C%22urn%5C%22%3A%20%5C%22bigtable%3Awrite%5C%22%2C%20%20%20%5C%22configurationParameters%5C%22%3A%20%7B%20%20%20%20%20%5C%22projectId%5C%22%3A%20%5C%22dataflow-syndeo%5C%22%2C%20%20%20%20%20%5C%22instanceId%5C%22%3A%20%5C%22syndeo-bt-test%5C%22%2C%20%20%20%20%20%5C%22tableId%5C%22%3A%20%5C%22syndeo-demo-table%5C%22%2C%20%20%20%20%20%5C%22keyColumns%5C%22%3A%20%5B%5C%22ride_id%5C%22%5D%20%20%20%7D%20%7D%20%7D%22%2C%22stagingLocation%22%3A%22gs%3A%2F%2Fsyndeo-pabloem%2Fbtdemo1%2Fstaging%2F%22%7D%2C%22environment%22%3A%7B%22tempLocation%22%3A%22gs%3A%2F%2Fsyndeo-pabloem%2Fbtdemo1%2Ftemp%2F%22%2C%22additionalExperiments%22%3A%5B%5D%7D%7D%7D%7D)
you can configure the parameters. Note that the **jsonSpecPayload** needs to be escaped and passed as a string (see the page).


Another option is via the command line ((pabloem) - I have not tested this successfully). You also need to provide an escaped JSON:

```sh
gcloud dataflow flex-template run ${JOB_NAME} \
    --template-file-gcs-location gs://$GCS_BUCKET_NAME/syndeo-template.json \
    --region ${REGION} --temp-location ${TEMP_LOCATION} \
    --parameters "jsonSpecPayload={ \"source\": {   \"urn\": \"bigquery:read\",   \"configurationParameters\" : {     \"table\": \"dataflow-syndeo.taxirides.realtime\"   }   }, \"sink\": {   \"urn\": \"bigtable:write\",   \"configurationParameters\": {     \"projectId\": \"dataflow-syndeo\",     \"instanceId\": \"syndeo-bt-test\",     \"tableId\": \"syndeo-demo-table\",     \"keyColumns\": [\"ride_id\"]   } } },stagingLocation=${TEMP_LOCATION}/staging/"
```

## Developing a new `SchemaTransformProvider` implementation

The Syndeo template inspects its classpath for implementations of `SchemaTransformProvider`.

### A `SchemaTransformProvider` in apache/beam

If you are interested in developing for Beam, check out the Beam wiki: https://cwiki.apache.org/confluence/display/BEAM/Developer+Guides

If you are developing a `SchemaTransformProvider` in Beam and you want to test it with a local Syndeo Template,
you need to install it into your local [Maven .m2 folder](https://stackoverflow.com/questions/63138495/what-is-m2-folder-how-can-i-configure-it-if-i-have-two-repos-with-two-differen/63139745#63139745).

To publish the module you are working with to your local `.m2` folder, you can run the `publishMavenJavaPublicationToMavenLocal`
task on the subproject, like so:

```sh
./gradlew :sdks:java:io:${SUBPROJECT}:publishMavenJavaPublicationToMavenLocal -Ppublishing
```

This will create the appropriate artifacts under `$USER/.m2/repository/org/apache/beam/beam-sdks-java-io-${SUBPROJECT}/`.
You can then amend the `beam.version` parameter in `syndeo-template/pom.xml` to use the latest snapshot (i.e. `2.${XX}.0-SNAPSHOT`)
for that Beam module. For example, for `sdks-java-io-google-cloud-platform`:

```xml
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-sdks-java-io-google-cloud-platform</artifactId>
  <version>2.46.0-SNAPSHOT</version>
</dependency>
```

**NOTE**: Usually it is good to only upgrade the library that you're testing, to avoid pulling in too many changes from Beam.
