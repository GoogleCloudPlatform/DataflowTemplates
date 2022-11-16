/*
 * Copyright (C) 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.templates;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.kafka.connector.KafkaIO;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.templates.KafkaToBigQuery.Options;
import com.google.cloud.teleport.templates.common.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.util.DualInputNestedValueProvider;
import com.google.cloud.teleport.util.DualInputNestedValueProvider.TranslatorInput;
import com.google.cloud.teleport.util.ResourceUtils;
import com.google.cloud.teleport.util.ValueProviderUtils;
import com.google.cloud.teleport.values.FailsafeElement;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link KafkaToBigQuery} pipeline is a streaming pipeline which ingests data in JSON format
 * from Kafka, executes a UDF, and outputs the resulting records to BigQuery. Any errors which occur
 * in the transformation of the data or execution of the UDF will be output to a separate errors
 * table in BigQuery. The errors table will be created if it does not exist prior to execution. Both
 * output and error tables are specified by the user as template parameters.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The Kafka topic exists and the message is encoded in a valid JSON format.
 *   <li>The BigQuery output table exists.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_ID=PROJECT ID HERE
 * BUCKET_NAME=BUCKET NAME HERE
 * PIPELINE_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/kafka-to-bigquery
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.KafkaToBigQuery \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --templateLocation=${PIPELINE_FOLDER}/template \
 * --runner=${RUNNER}"
 *
 * # Execute the template
 * JOB_NAME=kafka-to-bigquery-$USER-`date +"%Y%m%d-%H%M%S%z"`
 *
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template \
 * --zone=us-east1-d \
 * --parameters \
 * "bootstrapServers=my_host:9092,inputTopic=kafka-test,\
 * outputTableSpec=kafka-test:kafka.kafka_to_bigquery,\
 * outputDeadletterTable=kafka-test:kafka.kafka_to_bigquery_deadletter"
 * </pre>
 */
public class KafkaToBigQuery {

  /** The log to output status messages to. */
  private static final Logger LOG = LoggerFactory.getLogger(KafkaToBigQuery.class);

  /** The tag for the main output for the UDF. */
  public static final TupleTag<FailsafeElement<KV<String, String>, String>> UDF_OUT =
      new TupleTag<FailsafeElement<KV<String, String>, String>>() {};

  /** The tag for the main output of the json transformation. */
  public static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<TableRow>() {};

  /** The tag for the dead-letter output of the udf. */
  public static final TupleTag<FailsafeElement<KV<String, String>, String>> UDF_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<KV<String, String>, String>>() {};

  /** The tag for the dead-letter output of the json to table row transform. */
  public static final TupleTag<FailsafeElement<KV<String, String>, String>>
      TRANSFORM_DEADLETTER_OUT = new TupleTag<FailsafeElement<KV<String, String>, String>>() {};

  /** The default suffix for error tables if dead letter table is not specified. */
  public static final String DEFAULT_DEADLETTER_TABLE_SUFFIX = "_error_records";

  /**
   * The {@link Options} class provides the custom execution options passed by the executor at the
   * command-line.
   */
  public interface Options extends PipelineOptions, JavascriptTextTransformerOptions {
    @TemplateParameter.BigQueryTable(
        order = 1,
        description = "BigQuery output table",
        helpText =
            "BigQuery table location to write the output to. The name should be in the format "
                + "<project>:<dataset>.<table_name>. The table's schema must match input objects.")
    ValueProvider<String> getOutputTableSpec();

    void setOutputTableSpec(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 2,
        regexes = {"[,:a-zA-Z0-9._-]+"},
        description = "Kafka Bootstrap Server list",
        helpText = "Kafka Bootstrap Server list, separated by commas.",
        example = "localhost:9092,127.0.0.1:9093")
    ValueProvider<String> getBootstrapServers();

    void setBootstrapServers(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 3,
        regexes = {"[a-zA-Z0-9._-,]+"},
        description = "Kafka topic(s) to read input from.",
        helpText = "Kafka topic(s) to read input from.",
        example = "topic1,topic2")
    ValueProvider<String> getInputTopic();

    void setInputTopic(ValueProvider<String> value);

    @TemplateParameter.BigQueryTable(
        order = 4,
        optional = true,
        description = "The dead-letter table name to output failed messages to BigQuery",
        helpText =
            "Messages failed to reach the output table for all kind of reasons (e.g., mismatched "
                + "schema, malformed json) are written to this table. If it doesn't exist, it will be "
                + "created during pipeline execution.",
        example = "your-project-id:your-dataset.your-table-name")
    ValueProvider<String> getOutputDeadletterTable();

    void setOutputDeadletterTable(ValueProvider<String> value);
  }

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * KafkaToBigQuery#run(Options)} method to start the pipeline and invoke {@code
   * result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options. This method does not wait until the
   * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
   * object to block until the pipeline is finished running if blocking programmatic execution is
   * required.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(Options options) {

    Pipeline pipeline = Pipeline.create(options);

    // Register the coder for pipeline
    FailsafeElementCoder<KV<String, String>, String> coder =
        FailsafeElementCoder.of(
            KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()), StringUtf8Coder.of());

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    /*
     * Steps:
     *  1) Read messages in from Kafka
     *  2) Transform the Kafka Messages into TableRows
     *     - Transform message payload via UDF
     *     - Convert UDF result to TableRow objects
     *  3) Write successful records out to BigQuery
     *  4) Write failed records out to BigQuery
     */
    PCollectionTuple transformOut =
        pipeline
            /*
             * Step #1: Read messages in from Kafka
             */
            .apply(
                "ReadFromKafka",
                KafkaIO.<String, String>read()
                    .withBootstrapServers(options.getBootstrapServers())
                    .withTopic(options.getInputTopic())
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    // NumSplits is hard-coded to 1 for single-partition use cases (e.g., Debezium
                    // Change Data Capture). Once Dataflow dynamic templates are available, this can
                    // be deprecated.
                    .withNumSplits(1)
                    .withoutMetadata())

            /*
             * Step #2: Transform the Kafka Messages into TableRows
             */
            .apply("ConvertMessageToTableRow", new MessageToTableRow(options));

    /*
     * Step #3: Write the successful records out to BigQuery
     */
    transformOut
        .get(TRANSFORM_OUT)
        .apply(
            "WriteSuccessfulRecords",
            BigQueryIO.writeTableRows()
                .withoutValidation()
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .to(options.getOutputTableSpec()));

    /*
     * Step #4: Write failed records out to BigQuery
     */
    PCollectionList.of(transformOut.get(UDF_DEADLETTER_OUT))
        .and(transformOut.get(TRANSFORM_DEADLETTER_OUT))
        .apply("Flatten", Flatten.pCollections())
        .apply(
            "WriteFailedRecords",
            WriteKafkaMessageErrors.newBuilder()
                .setErrorRecordsTable(
                    ValueProviderUtils.maybeUseDefaultDeadletterTable(
                        options.getOutputDeadletterTable(),
                        options.getOutputTableSpec(),
                        DEFAULT_DEADLETTER_TABLE_SUFFIX))
                .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                .build());
    return pipeline.run();
  }

  static class KafkaRecordToFailsafeElementFn
      extends DoFn<KV<String, String>, FailsafeElement<KV<String, String>, String>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      KV<String, String> message = context.element();
      context.output(FailsafeElement.of(message, message.getValue()));
    }
  }

  /**
   * If deadletterTable is available, it is returned as is, otherwise outputTableSpec +
   * defaultDeadLetterTableSuffix is returned instead.
   */
  private static ValueProvider<String> maybeUseDefaultDeadletterTable(
      ValueProvider<String> deadletterTable,
      ValueProvider<String> outputTableSpec,
      String defaultDeadLetterTableSuffix) {
    return DualInputNestedValueProvider.of(
        deadletterTable,
        outputTableSpec,
        new SerializableFunction<TranslatorInput<String, String>, String>() {
          @Override
          public String apply(TranslatorInput<String, String> input) {
            String userProvidedTable = input.getX();
            String outputTableSpec = input.getY();
            if (userProvidedTable == null) {
              return outputTableSpec + defaultDeadLetterTableSuffix;
            }
            return userProvidedTable;
          }
        });
  }

  /**
   * The {@link MessageToTableRow} class is a {@link PTransform} which transforms incoming Kafka
   * Message objects into {@link TableRow} objects for insertion into BigQuery while applying an
   * optional UDF to the input. The executions of the UDF and transformation to {@link TableRow}
   * objects is done in a fail-safe way by wrapping the element with it's original payload inside
   * the {@link FailsafeElement} class. The {@link MessageToTableRow} transform will output a {@link
   * PCollectionTuple} which contains all output and dead-letter {@link PCollection}.
   *
   * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
   *
   * <ul>
   *   <li>{@link KafkaToBigQuery#UDF_OUT} - Contains all {@link FailsafeElement} records
   *       successfully processed by the optional UDF.
   *   <li>{@link KafkaToBigQuery#UDF_DEADLETTER_OUT} - Contains all {@link FailsafeElement} records
   *       which failed processing during the UDF execution.
   *   <li>{@link KafkaToBigQuery#TRANSFORM_OUT} - Contains all records successfully converted from
   *       JSON to {@link TableRow} objects.
   *   <li>{@link KafkaToBigQuery#TRANSFORM_DEADLETTER_OUT} - Contains all {@link FailsafeElement}
   *       records which couldn't be converted to table rows.
   * </ul>
   */
  static class MessageToTableRow
      extends PTransform<PCollection<KV<String, String>>, PCollectionTuple> {

    private final Options options;

    MessageToTableRow(Options options) {
      this.options = options;
    }

    @Override
    public PCollectionTuple expand(PCollection<KV<String, String>> input) {

      PCollectionTuple udfOut =
          input
              // Map the incoming messages into FailsafeElements so we can recover from failures
              // across multiple transforms.
              .apply("MapToRecord", ParDo.of(new MessageToFailsafeElementFn()))
              .apply(
                  "InvokeUDF",
                  FailsafeJavascriptUdf.<KV<String, String>>newBuilder()
                      .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                      .setFunctionName(options.getJavascriptTextTransformFunctionName())
                      .setSuccessTag(UDF_OUT)
                      .setFailureTag(UDF_DEADLETTER_OUT)
                      .build());

      // Convert the records which were successfully processed by the UDF into TableRow objects.
      PCollectionTuple jsonToTableRowOut =
          udfOut
              .get(UDF_OUT)
              .apply(
                  "JsonToTableRow",
                  FailsafeJsonToTableRow.<KV<String, String>>newBuilder()
                      .setSuccessTag(TRANSFORM_OUT)
                      .setFailureTag(TRANSFORM_DEADLETTER_OUT)
                      .build());

      // Re-wrap the PCollections so we can return a single PCollectionTuple
      return PCollectionTuple.of(UDF_OUT, udfOut.get(UDF_OUT))
          .and(UDF_DEADLETTER_OUT, udfOut.get(UDF_DEADLETTER_OUT))
          .and(TRANSFORM_OUT, jsonToTableRowOut.get(TRANSFORM_OUT))
          .and(TRANSFORM_DEADLETTER_OUT, jsonToTableRowOut.get(TRANSFORM_DEADLETTER_OUT));
    }
  }

  /**
   * The {@link MessageToFailsafeElementFn} wraps an Kafka Message with the {@link FailsafeElement}
   * class so errors can be recovered from and the original message can be output to a error records
   * table.
   */
  static class MessageToFailsafeElementFn
      extends DoFn<KV<String, String>, FailsafeElement<KV<String, String>, String>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      KV<String, String> message = context.element();
      context.output(FailsafeElement.of(message, message.getValue()));
    }
  }

  /**
   * The {@link WriteKafkaMessageErrors} class is a transform which can be used to write messages
   * which failed processing to an error records table. Each record is saved to the error table is
   * enriched with the timestamp of that record and the details of the error including an error
   * message and stacktrace for debugging.
   */
  @AutoValue
  public abstract static class WriteKafkaMessageErrors
      extends PTransform<PCollection<FailsafeElement<KV<String, String>, String>>, WriteResult> {

    public abstract ValueProvider<String> getErrorRecordsTable();

    public abstract String getErrorRecordsTableSchema();

    public static Builder newBuilder() {
      return new AutoValue_KafkaToBigQuery_WriteKafkaMessageErrors.Builder();
    }

    /** Builder for {@link WriteKafkaMessageErrors}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setErrorRecordsTable(ValueProvider<String> errorRecordsTable);

      public abstract Builder setErrorRecordsTableSchema(String errorRecordsTableSchema);

      public abstract WriteKafkaMessageErrors build();
    }

    @Override
    public WriteResult expand(
        PCollection<FailsafeElement<KV<String, String>, String>> failedRecords) {

      return failedRecords
          .apply("FailedRecordToTableRow", ParDo.of(new FailedMessageToTableRowFn()))
          .apply(
              "WriteFailedRecordsToBigQuery",
              BigQueryIO.writeTableRows()
                  .to(getErrorRecordsTable())
                  .withJsonSchema(getErrorRecordsTableSchema())
                  .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                  .withWriteDisposition(WriteDisposition.WRITE_APPEND));
    }
  }

  /**
   * The {@link FailedMessageToTableRowFn} converts Kafka message which have failed processing into
   * {@link TableRow} objects which can be output to a dead-letter table.
   */
  public static class FailedMessageToTableRowFn
      extends DoFn<FailsafeElement<KV<String, String>, String>, TableRow> {

    /**
     * The formatter used to convert timestamps into a BigQuery compatible <a
     * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#timestamp-type">format</a>.
     */
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
        DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    @ProcessElement
    public void processElement(ProcessContext context) {
      FailsafeElement<KV<String, String>, String> failsafeElement = context.element();
      final KV<String, String> message = failsafeElement.getOriginalPayload();

      // Format the timestamp for insertion
      String timestamp =
          TIMESTAMP_FORMATTER.print(context.timestamp().toDateTime(DateTimeZone.UTC));

      // Build the table row
      final TableRow failedRow =
          new TableRow()
              .set("timestamp", timestamp)
              .set("errorMessage", failsafeElement.getErrorMessage())
              .set("stacktrace", failsafeElement.getStacktrace());

      // Only set the payload if it's populated on the message.
      failedRow.set(
          "payloadString",
          "key: "
              + (message.getKey() == null ? "" : message.getKey())
              + "value: "
              + (message.getValue() == null ? "" : message.getValue()));
      context.output(failedRow);
    }
  }
}
