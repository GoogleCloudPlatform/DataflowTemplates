# Adding an Integration or Load Test

## Overview

Write a load test for an Apache Beam pipeline on Dataflow using JUnit and gather 
performance metrics. This will take you through setting up the BigQuery table 
and writing the load test. Once done, you'll be able to stress test your pipeline.

## Pre-requisites

1. Know how to write Apache beam pipelines.
2. Know how to write JUnit tests.

## Google Cloud Resources

1. Dataflow
2. BigQuery
3. Google Cloud Storage (GCS)

## Build necessary artifacts

To run a load test you will first need to write the pipeline under test and create 
the appropriate artifacts. 

* For dataflow template pipelines, the template will have to be created. Please use 
  the following tutorials to build the template artifacts - [flex template](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates), [classic template](https://cloud.google.com/dataflow/docs/guides/templates/creating-templates).

## Set up BigQuery table
This framework allows users to export performance metrics measured to a
BigQuery table. The data exported will include various performance metrics
measured along with the job configuration. While optional, many use cases will
require this step.

Follow these steps to set up the BQ table for exporting results,

1. [Create a BigQuery dataset](https://cloud.google.com/bigquery/docs/datasets)
2. Create a BigQuery table with the following schema,

```json
[
    {
        "name": "timestamp",
        "description": "The timestamp when Dataflow job created",
        "type": "TIMESTAMP",
        "mode": "REQUIRED"
    },
    {
        "name": "sdk",
        "description": "Beam SDK used to run job",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "version",
        "description": "Beam SDK version used to run job",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "job_type",
        "description": "Streaming or batch job",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "template_name",
        "description": "The name of the Dataflow template, if applicable",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "template_version",
        "description": "The version of the Dataflow template, if applicable",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "template_type",
        "description": "Classic or flex template, if applicable",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "test_name",
        "description": "The name ascribed to the executed test",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "parameters",
        "description": "Test related parameters",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {
                "name": "name",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "value",
                "type": "STRING",
                "mode": "REQUIRED"
            }
        ]
    },
    {
        "name": "metrics",
        "description": "Metrics queried using the Dataflow API and Cloud monitoring",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {
                "name": "name",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "value",
                "type": "FLOAT",
                "mode": "REQUIRED"
            }
        ]
    }
]
```

Also, don’t forget to set up permissions to write to the BigQuery table.


## Test Components

### Resource Manager
A pre-requisite to writing a load test for a specific pipeline is to set up the
source and sink. A Resource manager can be used to dynamically create and tear down
resources for tests. Resource managers should usually be set up in the
[Before](https://junit.org/junit4/javadoc/4.13/org/junit/Before.html) method and cleaned up in the [After](https://junit.org/junit4/javadoc/4.12/org/junit/After.html) method.
Resources will be created in each test as required.

For example, a Pub/Sub resource manager can be created the following way

```java
private static PubsubResourceManager pubsubResourceManager;

@Before
public void setup() throws IOException{
  pubsubResourceManager =
    DefaultPubsubResourceManager.builder(testName, PROJECT)
      .credentialsProvider(CREDENTIALS_PROVIDER)
      .build();
}

@After
public void tearDown() {
  ResourceManagerUtils.cleanResources(pubsubResourceManager);
}

@Test
public void testBacklog() {
  // create topic
  TopicName backlogTopic = pubsubResourceManager.createTopic("backlog-input");
}
```

### Pipeline Launcher

A pipeline launcher is a client for launching Beam pipelines. Currently, only
Dataflow runners (Legacy and Runner V2) and Direct runner is supported.

The pipeline is launched using the `LaunchConfig` specified. Parameters can be
passed in using `addParameter` method, whereas environment can be passed using
`addEnvironment`.


#### Dataflow Templates

To launch a dataflow template pipeline, the appropriate pipeline launcher has to
be used. The provided launchers are,
* ClassicTemplateClient - Client for interacting with Dataflow Classic Templates
* FlexTemplateClient - Client for interacting with Dataflow Flex Templates.
* DirectRunnerClient - Client for launching Dataflow templates on DirectRunner. 

If ClassicTemplateClient or FlexTemplateClient is used, a spec path also needs 
to be provided (along with all necessary parameters).

For example,
```java
// build flex template pipeline launcher
PipelineLauncher pipelineLauncher = FlexTemplateClient.builder().setCredentials(credentials).build();

// config for pipeline under test
LaunchConfig options =
    LaunchConfig.builder(testName, SPEC_PATH)
      .addEnvironment("maxWorkers", 50)
      .addParameter("inputSubscription", backlogSubscription.toString())
      .addParameter("outputTableSpec", toTableSpec(project, table))
      .addParameter("autoscalingAlgorithm", "THROUGHPUT_BASED")
      .build()
// launch pipeline under test
LaunchInfo info = pipelineLauncher.launch(project, region, options);
assertThatPipeline(info).isRunning();

```

#### Non-Template Pipelines

For launching regular beam pipelines the `DefaultPipelineLauncher` should be 
used. 

##### Java SDK pipelines

To launch a Java SDK pipeline, a Pipeline object needs to be passed along with
all necessary parameters.

NOTE: DefaultPipelineLauncher supports running Java pipelines on DirectRunner. 
This can be done by changing the `runner` parameter to `DirectRunner`.

For example,
```java
  @Rule public TestPipeline wcPipeline = TestPipeline.create();

  /** Build WordCount pipeline. */
  private void buildPipeline() {
    wcPipeline
        .apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
        .apply(
            FlatMapElements.into(TypeDescriptors.strings())
                .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
        .apply(Filter.by((String word) -> !word.isEmpty()))
        .apply(Count.perElement())
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (KV<String, Long> wordCount) ->
                        wordCount.getKey() + ": " + wordCount.getValue()))
        .apply(TextIO.write().to("wordcounts"));
  }

  @Test
  public void testWordCountDataflow() throws IOException {
    buildPipeline();
    
    // build DefaultPipelineLauncher to launch regular dataflow jobs
    PipelineLauncher pipelineLauncher = DefaultPipelineLauncher.builder().setCredentials(CREDENTIALS).build();
    
    LaunchConfig options = LaunchConfig.builder("test-wordcount")
      .setSdk(Sdk.JAVA)
      .setPipeline(wcPipeline)
      .addParameter("runner", "DataflowRunner")
      .build();

    LaunchInfo launchInfo = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(launchInfo).isRunning();
    ...
    }
```

### Pipeline Operator

Pipeline operator provides utilities for running operations and managing
dataflow jobs.

Some utilities provided are,

* `waitUntilDone` -  Waits until the given job is done, timing out it if runs for too long.
  ```java
  // wait until completion for 20 minutes or timeout
  Result result = pipelineOperator.waitUntilDone(createConfig(launchInfo, Duration.ofMinutes(20)));
  // assert that pipeline completed successfully
  assertThatResult(result).isLaunchFinished();
  ```
* `waitForConditionAndFinish` - Waits until a given condition is met OR when
  the job enters a state that indicates that it is done or ready to be done. If
  the condition was met before the job entered a done or finishing state, then
  this will drain the job and wait for the job to enter a done state.

  ```java
  // wait until certain amount of messages reach the Pub/Sub subscription.
  Result result =
    pipelineOperator.waitForConditionAndFinish(
      createConfig(info, Duration.ofMinutes(20)),
        () -> {
          Long currentMessages =
              monitoringClient.getNumMessagesInSubscription(
                  project, outputSubscription.getSubscription());
          LOG.info(
              "Found {} messages in output subscription, expected {} messages.",
              currentMessages,
              expectedMessages);
          return currentMessages != null && currentMessages >= expectedMessages;
        });

  // Assert
  assertThatResult(result).meetsConditions();
  ```
* `waitForConditionAndCancel` - Waits until a given condition is met OR when
  the job enters a state that indicates that it is done or ready to be done. If
  the condition was met before the job entered a done or finishing state, then
  this will cancel the job and wait for the job to enter a done state.

### Data generation
This framework provides a `DataGenerator` to easily generate synthetic data for tests.
The data generator supports generating either unlimited or fixed number of
synthetic records/messages at a user specified QPS. These messages are generated
based on a user specified schema. Please look at [this](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/v2/streaming-data-generator#creating-the-schema-file)
guide to create a schema file.

Data generators are typically used in load tests, not integration tests.

Pre-existing schema templates can also be used instead of specifying a schema. 
Supported schema templates are,

* GAME_EVENT
  ```json
  {
      "eventId": "{{uuid()}}",
      "eventTimestamp": {{timestamp()}},
      "ipv4": "{{ipv4()}}",
      "ipv6": "{{ipv6()}}",
      "country": "{{country()}}",
      "username": "{{username()}}",
      "quest": "{{random("A Break In the Ice", "Ghosts of Perdition", "Survive the Low Road")}}",
      "score": {{integer(100, 10000)}},
      "completed": {{bool()}}
  }
  ```
* LOG_ENTRY
  ```json
  {
    "logName": "{{alpha(10,20)}}",
    "resource": {
      "type": "{{alpha(5,10)}}"
    },
    "timestamp": {{timestamp()}},
    "receiveTimestamp": {{timestamp()}},
    "severity": "{{random("DEFAULT", "DEBUG", "INFO", "NOTICE", "WARNING", "ERROR", "CRITICAL", "ERROR")}}",
    "insertId": "{{uuid()}}",
    "trace": "{{uuid()}}",
    "spanId": "{{uuid()}}",
    "jsonPayload": {
      "bytes_sent": {{integer(1000,20000)}},
      "connection": {
        "dest_ip": "{{ipv4()}}",
        "dest_port": {{integer(0,65000)}},
        "protocol": {{integer(0,6)}},
        "src_ip": "{{ipv4()}}",
        "src_port": {{integer(0,65000)}}
      },
      "dest_instance": {
        "project_id": "{{concat("PROJECT", integer(0,3))}}",
        "region": "{{country()}}",
        "vm_name": "{{username()}}",
        "zone": "{{state()}}"
      },
      "end_time": {{timestamp()}},
      "packets_sent": {{integer(100,400)}},
      "reporter": "{{random("SRC", "DEST")}}",
      "rtt_msec": {{integer(0,20)}},
      "start_time": {{timestamp()}}
    }
  }
  ```

Currently, data generator supports writing synthetic data to the following sinks,

1. Pub/Sub
2. BigQuery
3. Google Cloud Storage (GCS)
4. Spanner
5. Jdbc

NOTE: DataGenerator.execute(...) is a blocking call. If a messageLimit
is specified, the data generator finishes after generating the specified number 
of messages. If messageLimit is not specified, the data generator generates 
messages at specified QPS till timeout.


## Write an Integration test

Integration tests will be written using JUnit. The structure of the load test will
vary on whether the pipeline under test is a `Batch` or `Streaming` pipeline and
the type of test.

### Structure
First extend the test class from the [TemplateTestBase](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/it/google-cloud-platform/src/main/java/org/apache/beam/it/gcp/TemplateTestBase.java)
class. TemplateTestBase contains helper methods which abstract irrelevant 
information and make it easier to write tests. It also defines some 
clients and variables which are useful for writing tests.

```java
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.junit.runners.JUnit4;
@Category({TemplateIntegrationTest.class})
@TemplateIntegrationTest(WordCount.class)
@RunWith(JUnit4.class)
public class WordCountIT extends TemplateTestBase {

}
```

From there, you can add test cases as described below in [Test Cases](#test-cases).

## Write a Load test

Load tests will be written using JUnit. The structure of the load test will
vary on whether the pipeline under test is a `Batch` or `Streaming` pipeline and
the type of test.

### Structure
First extend the test class from the [LoadTestBase](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/it/google-cloud-platform/src/main/java/org/apache/beam/it/gcp/LoadTestBase.java)
class. LoadTestBase contains helper methods which abstract irrelevant 
information and make it easier to write load tests. It also defines some 
clients and variables which are useful for writing tests.

NOTE: Any class extending `LoadTestBase` will need to implement a `launcher` 
method which creates the appropriate PipelineLauncher to be used for the test.

```java
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.cloud.teleport.it.TemplateLoadTestBase;
import org.junit.runners.JUnit4;
@Category(TemplateLoadTest.class)
@TemplateLoadTest(WordCount.class)
@RunWith(JUnit4.class)
public class WordCountLoadTest extends TemplateLoadTestBase {

  PipelineLauncher launcher() {
    return new DefaultPipelineLauncher.builder().setCredentials(CREDENTIALS).build();
  }
  
}

```

NOTE: For Google-provided template load tests, `TemplateLoadTestBase` can be used, whereas for 
Apache Beam I/O load tests `IOLoadTestBase` can be used.

From there, you can add test cases as described below in [Test Cases](#test-cases).

## Test cases

There are generally 2 classes of load and integration tests: backlog tests and steady state
tests (streaming-only). These largely function in the same manner, with minor differences
which are called out in the code below.

#### Backlog Tests

In a backlog test, data is accumulated to be processed before the pipeline
starts. Hence, we will need to generate data before launching the pipeline under
test.

For example, a backlog test for Pub/Sub to BigQuery template would look like

```java
@Test
public void testBacklog() {
  // create resources
  TopicName backlogTopic = pubsubResourceManager.createTopic("backlog-input");
  SubscriptionName backlogSubscription = 
    pubsubResourceManager.createSubscription(backlogTopic, "backlog-subscription");
  TableId table = bigQueryResourceManager.createTable(testName, SCHEMA);
  
  // Generate fake data to Pub/Sub topic
  // In a normal integration test (small amount of data), you can use the resource manager
  // directly. For example:
  // pubsubResourceManager.publish(topic, ImmutableMap.of(), messageData);
  DataGenerator dataGenerator =
       DataGenerator.builderWithSchemaTemplate(testName + "-data-generator", "GAME_EVENT")
           .setQPS("200000")
           .setMessagesLimit(String.valueOf(numMessages))
           .setTopic(backlogTopic.toString())
           .setNumWorkers("10")
           .setMaxNumWorkers("20")
           .build();
  dataGenerator.execute(Duration.ofMinutes(30));

  LaunchConfig options = LaunchConfig.builder(testName, SPEC_PATH)
    .addParameter("inputSubscription", inputSubscription.toString())
    .addParameter("outputTableSpec", toTableSpec(project, table))
    .build();
  // Launch pipeline under test. For load tests, use the pipelineLauncher
  // For integration tests, you can use launchTemplate, for example:
  // LaunchInfo info = launchTemplate(options)
  LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
  assertThatPipeline(info).isRunning();

  // use pipelineOperator to wait until all messages reach BigQuery, then drain the job
  Result result = pipelineOperator.waitForConditionAndFinish(
    createConfig(info, Duration.ofMinutes(40)),
    BigQueryRowsCheck.builder(bigQueryResourceManager, table)
      .setMinRows(NUM_MESSAGES)
      .build());

  // Assert
  assertThatResult(result).meetsConditions();
}
```

#### Steady State test (Streaming pipelines only)

In a steady state test, we test the pipeline by steadily generating data to the 
source at a specified QPS over a specified duration (e.g. 1 hour run).

For example, a steady-state test at 100,000 QPS for 1hr for Pub/Sub to BigQuery template would look like
```java
@Test
public void testSteadyState1hr() {
  // Create resources
  TopicName inputTopic = pubsubResourceManager.createTopic("steady-state-input");
  SubscriptionName inputSubscription = 
    pubsubResourceManager.createSubscription(inputTopic, "steady-state-subscription");
  TableId table = bigQueryResourceManager.createTable(
        testName, SCHEMA, System.currentTimeMillis() + 7200000); // expire in 2 hrs
  // Generate fake data to Pub/Sub topic at 100,000 QPS
  // In a normal integration test (small amount of data), you can use the resource manager
  // directly. For example:
  // pubsubResourceManager.publish(topic, ImmutableMap.of(), messageData);
  DataGenerator dataGenerator = 
    DataGenerator.builderWithSchemaTemplate(testName + "-data-generator","GAME_EVENT")
      .setQPS("100000")
      .setTopic(inputTopic.toString())
      .setNumWorkers("10")
      .setMaxNumWorkers("100")
      .build();

  LaunchConfig options = LaunchConfig.builder(testName, SPEC_PATH)
    .addParameter("inputSubscription", inputSubscription.toString())
    .addParameter("outputTableSpec", toTableSpec(project, table))
    .build();
  // Launch pipeline under test. For load tests, use the pipelineLauncher
  // For integration tests, you can use launchTemplate, for example:
  // LaunchInfo info = launchTemplate(options)
  LaunchInfo info = pipelineLauncher.launch(project, region, options);
  assertThatPipeline(info).isRunning();
  
  // Execute data generator for 1 hr
  // ElementCount metric in dataflow is approximate, allow for 1% difference
  int expectedMessages = (int) (dataGenerator.execute(Duration.ofMinutes(60)) * 0.99);

  // use pipelineOperator to wait until all messages reach BigQuery, then drain the job
  Result result = pipelineOperator.waitForConditionAndFinish(
    createConfig(info, Duration.ofMinutes(20)),
    BigQueryRowsCheck.builder(bigQueryResourceManager, table)
      .setMinRows(expectedMessages)
      .build());
  
  // Assert
  assertThatResult(result).meetsConditions();
}
```

### Exporting Results

After the pipeline finishes successfully, we can get the performance metrics using [getMetrics](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/it/google-cloud-platform/src/main/java/org/apache/beam/it/gcp/LoadTestBase.java#L279)
method and export the results to BigQuery by calling the [exportMetricsToBigQuery](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/it/google-cloud-platform/src/main/java/org/apache/beam/it/gcp/LoadTestBase.java#L139) method.

The BigQuery project, dataset, and table to be used to export the data can be specified in the command line using,
* `-DexportProject` - BigQuery Project to export metrics (optional, if not provided `-Dproject` is used)
* `-DexportDataset` - BigQuery dataset to export metrics
* `-DexportTable` - BigQuery table to export metrics

If the above information is not passed, the metrics data is printed but not exported.

```java
// export results
@Test
public void testBacklog(){
  ...
  exportMetricsToBigQuery(info,getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
}
```

## Run the Test
For manually running a load test execute the following commands on the CLI use the following commands,

1. Authenticate using the gcloud command
   ```shell
   gcloud auth application-default login
   ```
   or use the following command to generate a 60 min access token
   ```shell
   export DT_IT_ACCESS_TOKEN=$(gcloud auth application-default print-access-token)
   ```
2. Run the specific test
   ```shell
   mvn test -Dtest="<test-class>#<test-method>" -Dproject=$PROJECT \
    -DartifactBucket=$ARTIFACT_BUCKET -DexportProject=$EXPORT_PROJECT \
    -DexportDataset=$EXPORT_DATASET -DexportTable=$EXPORT_TABLE \
    -DfailIfNoTests=false -DtrimStackTrace=false
   ```

   Additional parameters can be specified using  `-D<param-name>=<param-value>`

To run an integration test, follow step 1 above, then run the specific test:

```shell
mvn clean verify \
  -PtemplatesIntegrationTests \
  -Dtest="<test-class>#<test-method>" -Dproject=$PROJECT \
  -DartifactBucket=$ARTIFACT_BUCKET -DexportProject=$EXPORT_PROJECT \
  -DexportDataset=$EXPORT_DATASET -DexportTable=$EXPORT_TABLE \
  -DfailIfNoTests=false -DtrimStackTrace=false
```

