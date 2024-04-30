# Code Contribution Guide

This guide outlines everything you need to know before making your first
contribution to this repo. If you find any missing information, please update
this guide after you find an answer!

If you are a repository maintainer and need info on repo governance,
how to review/merge code, or other info, see the
[Maintainers Guide](./maintainers-guide.md).

## Prerequisites

If you are not already familiar with Dataflow, Dataflow Templates (especially
flex templates), and Apache Beam it is recommended that you familarize yourself
with each of these at a high level before contributing. Information on each of
these can be found in the following locations:

* [Dataflow](https://cloud.google.com/dataflow/docs/overview) - general Dataflow documentation.
* [Dataflow Templates](https://cloud.google.com/dataflow/docs/concepts/dataflow-templates) - basic template concepts.
* [Flex templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates) - the preferred way to build new templates
* [Google-provided Templates](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates) - official documentation for templates provided by Google (the source code is in this repository).
* [Apache Beam](https://beam.apache.org)
  - [Overview](https://beam.apache.org/use/beam-overview/)
  - Quickstart: [Java](https://beam.apache.org/get-started/quickstart-java), [Python](https://beam.apache.org/get-started/quickstart-py), [Go](https://beam.apache.org/get-started/quickstart-go), [Yaml overview](https://beam.apache.org/documentation/sdks/yaml/)
  - [Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/)

## Getting Started

### Requirements

* Java 11
* Maven 3
* IntelliJ (recommended) or another editor of your choice

### IntelliJ & IDE Setup

For v1 templates, just open the project at the root directory and use maven
from there.

For v2 templates, open the project at the root directory then find the
"Add Maven Project" action and add the v2 directory and let the project rebuild.

### Maven Commands

All Maven commands should be run on the parent POM.
For example:

```
mvn clean package -pl v2/pubsub-binary-to-bigquery -am
```

### Building/Testing from IntelliJ

IntelliJ, by default, will often skip necessary Maven goals, leading to build
failures. You can fix these in the Maven view by going to
**Module_Name > Plugins > Plugin_Name** where Module_Name and Plugin_Name are
the names of the respective module and plugin with the rule. From there,
right-click the rule and select "Execute Before Build".

The list of known rules that require this are:

* common > Plugins > protobuf > protobuf:compile
* common > Plugins > protobuf > protobuf:test-compile

### Building the Project

Build the entire project using the maven compile command.

```sh
mvn clean compile
```

### Formatting Code

From either the root directory or v2/ directory, run:

```sh
mvn spotless:apply
```

This will format the code and add a license header. To verify that the code is
formatted correctly, run:

```sh
mvn spotless:check
```

### Executing a Template File

Once the template is staged on Google Cloud Storage, it can then be executed
using the
gcloud CLI tool. Please check [Running classic templates](https://cloud.google.com/dataflow/docs/guides/templates/running-templates#using-gcloud)
or [Using Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates#run-a-flex-template-pipeline)
for more information.

## Developing/Contributing Templates

### Templates Plugin

Templates plugin was created to make the workflow of creating, testing and
releasing Templates easier.

Before using the plugin, please make sure that
the [gcloud CLI](https://cloud.google.com/sdk/docs/install) is installed and
up-to-date, and that the client is properly authenticated using:

```shell
gcloud init
gcloud auth application-default login
```

After authenticated, install the plugin into your local repository:

```shell
mvn clean install -pl plugins/templates-maven-plugin -am
```

### Staging (Deploying) Templates

To stage a Template, it is necessary to upload the images to Artifact
Registry (for Flex templates) and copy the template to Cloud Storage.

Although there are different steps that depend on the kind of template being
developed. The plugin allows a template to be staged using the following single
command:

```shell
mvn clean package -PtemplatesStage  \
  -DskipTests \
  -DprojectId="{projectId}" \
  -DbucketName="{bucketName}" \
  -DstagePrefix="images/$(date +%Y_%m_%d)_01" \
  -DtemplateName="Cloud_PubSub_to_GCS_Text_Flex" \
  -pl v2/googlecloud-to-googlecloud -am
```

Notes:
- Change `-pl v2/googlecloud-to-googlecloud` and `-DtemplateName` to point to the specific Maven module where your template is located. Even though `-pl` is not required, it allows the command to run considerably faster.
- In case `-DtemplateName` is not specified, all templates for the module will be staged.

### Generating Template's Terraform module

This repository can generate a [terraform module](https://developer.hashicorp.com/terraform/language/modules)
that prompts users for template specific parameters and launch a Dataflow Job.
To generate a template specific terraform module, see the instructions for classic
and flex templates below.

#### Plugin artifact dependencies

The required plugin artifact dependencies are listed below:
- [plugins/core-plugin/src/main/resources/terraform-classic-template.tf](plugins/core-plugin/src/main/resources/terraform-classic-template.tf)
- [plugins/core-plugin/src/main/resources/terraform-classic-template.tf](plugins/core-plugin/src/main/resources/terraform-classic-template.tf)

These are outputs from the [cicd/cmd/run-terraform-schema](cicd/cmd/run-terraform-schema).
See [cicd/cmd/run-terraform-schema/README.md](cicd/cmd/run-terraform-schema/README.md)
for further details.

#### For Classic templates:

```shell
mvn clean prepare-package \
  -DskipTests \
  -PtemplatesTerraform \
  -pl v1 -am
```

Next, [terraform fmt](https://developer.hashicorp.com/terraform/cli/commands/fmt) the modules after
generating:
```shell
terraform fmt -recursive v1
```

The resulting terraform modules are generated in [v1/terraform](v1/terraform).

#### For Flex templates:

```shell
mvn clean prepare-package \
  -DskipTests \
  -PtemplatesTerraform \
  -pl v2/googlecloud-togooglecloud -am
```

Next, [terraform fmt](https://developer.hashicorp.com/terraform/cli/commands/fmt) the modules after
generating:
```shell
terraform fmt -recursive v2
```

The resulting terraform modules are generated in `v2/<source>-to-<sink>/terraform`,
for example [v2/bigquery-to-bigtable/terraform](v2/bigquery-to-bigtable/terraform).

Notes:
- Change `-pl v2/googlecloud-to-googlecloud` and `-DtemplateName` to point to the specific Maven module where your template is located.

### Running a Template

A template can also be executed on Dataflow, directly from the command line. The
command-line is similar to staging a template, but it is required to
specify `-Dparameters` with the parameters that will be used when launching the
template. For example:

```shell
mvn clean package -PtemplatesRun \
  -DskipTests \
  -DprojectId="{projectId}" \
  -DbucketName="{bucketName}" \
  -Dregion="us-central1" \
  -DtemplateName="Cloud_PubSub_to_GCS_Text_Flex" \
  -Dparameters="inputTopic=projects/{projectId}/topics/{topicName},windowDuration=15s,outputDirectory=gs://{outputDirectory}/out,outputFilenamePrefix=output-,outputFilenameSuffix=.txt" \
  -pl v2/googlecloud-to-googlecloud -am

```

Notes:
- When running a template, `-DtemplateName` is mandatory, as `-Dparameters=` are
  different across templates.
- `-PtemplatesRun` is self-contained, i.e., it is not required to run **
  Deploying/Staging Templates** before. In case you want to run a previously
  staged template, the existing path can be provided
  as `-DspecPath=gs://.../path`
- `-DjobName="{name}"` may be informed if a specific name is desirable (
  optional).
- If you encounter the error `Template run failed: File too large`, try adding `-DskipShade` to the mvn args.


### Running Integration Tests

To run integration tests, the developer plugin can be also used to stage template on-demand (in case the parameter `-DspecPath=` is not specified).

For example, to run all the integration tests in a specific module (in the example below, `v2/googlecloud-to-googlecloud`):

```shell
mvn clean verify \
  -PtemplatesIntegrationTests \
  -Dproject="{project}" \
  -DartifactBucket="{bucketName}" \
  -Dregion=us-central1 \
  -pl v2/googlecloud-to-googlecloud -am
```

The parameter `-Dtest=` can be given to test a single class (e.g., `-Dtest=PubsubToTextIT`) or single test case (e.g., `-Dtest=PubsubToTextIT#testTopicToGcs`).

The same happens when the test is executed from an IDE, just make sure to add the parameters `-Dproject=`, `-DartifactBucket=` and `-Dregion=` as program or VM arguments.

## Metadata Annotations

A template requires more information than just a name and description. For
example, in order to be used from the Dataflow UI, parameters need a longer help
text to guide users, as well as proper types and validations to make sure
parameters are being passed correctly.

We introduced annotations to have the source code as a single source of truth,
along with a set of utilities / plugins to generate template-accompanying
artifacts (such as command specs, parameter specs).

#### @Template Annotation

Every template must be annotated with `@Template`. Existing templates can be
used for reference, but the structure is as follows:

```java

@Template(
    name = "BigQuery_to_Elasticsearch",
    category = TemplateCategory.BATCH,
    displayName = "BigQuery to Elasticsearch",
    description = "A pipeline which sends BigQuery records into an Elasticsearch instance as JSON documents.",
    optionsClass = BigQueryToElasticsearchOptions.class,
    flexContainerName = "bigquery-to-elasticsearch")
public class BigQueryToElasticsearch {
```

#### @TemplateParameter Annotation

A set of `@TemplateParameter.{Type}` annotations were created to allow the
definition of options for a template, and the proper rendering in the UI, and
validations by the template launch service. Examples can be found in the
repository, but the general structure is as follows:

```java
@TemplateParameter.Text(
    order = 2,
    optional = false,
    regexes = {"[,a-zA-Z0-9._-]+"},
    description = "Kafka topic(s) to read the input from",
    helpText = "Kafka topic(s) to read the input from.",
    example = "topic1,topic2")
@Validation.Required
String getInputTopics();
```

```java
@TemplateParameter.GcsReadFile(
    order = 1,
    description = "Cloud Storage Input File(s)",
    helpText = "Path of the file pattern glob to read from.",
    example = "gs://your-bucket/path/*.csv")
String getInputFilePattern();
```

```java
@TemplateParameter.Boolean(
    order = 11,
    optional = true,
    description = "Whether to use column alias to map the rows.",
    helpText = "If enabled (set to true) the pipeline will consider column alias (\"AS\") instead of the column name to map the rows to BigQuery.")
@Default.Boolean(false)
Boolean getUseColumnAlias();
```

```java
@TemplateParameter.Enum(
    order = 21,
    enumOptions = {"INDEX", "CREATE"},
    optional = true,
    description = "Build insert method",
    helpText = "Whether to use INDEX (index, allows upsert) or CREATE (create, errors on duplicate _id) with Elasticsearch bulk requests.")
@Default.Enum("CREATE")
BulkInsertMethodOptions getBulkInsertMethod();
```

Note: `order` is relevant for templates that can be used from the UI, and
specify the relative order of parameters.

#### @TemplateIntegrationTest Annotation

This annotation should be used by classes that are used for integration tests of
other templates. This is used to wire a specific `IT` class with a template, and
allows environment preparation / proper template staging before tests are
executed on Dataflow.

Template tests have to follow this general format (please note
the `@TemplateIntegrationTest` annotation and the `TemplateTestBase`
super-class):

```java

@TemplateIntegrationTest(PubsubToText.class)
@RunWith(JUnit4.class)
public final class PubsubToTextIT extends TemplateTestBase {
```

Please refer to `Templates Plugin` to use and validate such annotations.

## Using UDFs

User-defined functions (UDFs) allow you to customize a template's functionality
by providing a short JavaScript function without having to maintain the entire
codebase. This is useful in situations which you'd like to rename fields, filter
values, or even transform data formats before output to the destination. All
UDFs are executed by providing the payload of the element as a string to the
JavaScript function. You can then use JavaScript's in-built JSON parser or other
system functions to transform the data prior to the pipeline's output. The
return statement of a UDF specifies the payload to pass forward in the pipeline.
This should always return a string value. If no value is returned or the
function returns undefined, the incoming record will be filtered from the
output.

### UDF Function Specification

| Template              | UDF Input Type | Input Description                               | UDF Output Type | Output Description                                                                                     |
|-----------------------|----------------|-------------------------------------------------|-----------------|--------------------------------------------------------------------------------------------------------|
| Datastore Bulk Delete | String         | A JSON string of the entity                     | String          | A JSON string of the entity to delete; filter entities by returning undefined                          |
| Datastore to Pub/Sub  | String         | A JSON string of the entity                     | String          | The payload to publish to Pub/Sub                                                                      |
| Datastore to GCS Text | String         | A JSON string of the entity                     | String          | A single-line within the output file                                                                   |
| GCS Text to BigQuery  | String         | A single-line within the input file             | String          | A JSON string which matches the destination table's schema                                             |
| Pub/Sub to BigQuery   | String         | A string representation of the incoming payload | String          | A JSON string which matches the destination table's schema                                             |
| Pub/Sub to Datastore  | String         | A string representation of the incoming payload | String          | A JSON string of the entity to write to Datastore                                                      |
| Pub/Sub to Splunk     | String         | A string representation of the incoming payload | String          | The event data to be sent to Splunk HEC events endpoint. Must be a string or a stringified JSON object |

### UDF Examples

For a comprehensive list of samples, please check our [udf-samples](v2/common/src/main/resources/udf-samples) folder.

#### Adding fields

```js
/**
 * A transform which adds a field to the incoming data.
 * @param {string} inJson
 * @return {string} outJson
 */
function transform(inJson) {
  var obj = JSON.parse(inJson);
  obj.dataFeed = "Real-time Transactions";
  obj.dataSource = "POS";
  return JSON.stringify(obj);
}
```

#### Filtering records

```js
/**
 * A transform function which only accepts 42 as the answer to life.
 * @param {string} inJson
 * @return {string} outJson
 */
function transform(inJson) {
  var obj = JSON.parse(inJson);
  // only output objects which have an answer to life of 42.
  if (obj.hasOwnProperty('answerToLife') && obj.answerToLife === 42) {
    return JSON.stringify(obj);
  }
}
```

## Generated Documentation

This repository contains generated documentation, which contains a list of parameters
and instructions on how to customize and/or build every template.

To generate the documentation for all templates, the following command can be used:

```shell
mvn clean prepare-package \
  -DskipTests \
  -PtemplatesSpec
```

## Release Process

Templates are released in a weekly basis (best-effort) as part of the efforts to
keep [Google-provided Templates](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates)
updated with latest fixes and improvements.

To learn more about this process, or how you can stage your own changes, see [Release Process](./release-process.md).
