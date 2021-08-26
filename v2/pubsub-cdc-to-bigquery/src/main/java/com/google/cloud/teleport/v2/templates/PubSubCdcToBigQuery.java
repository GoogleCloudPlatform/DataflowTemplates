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
package com.google.cloud.teleport.v2.templates;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.cdc.dlq.BigQueryDeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.cdc.dlq.StringDeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.cdc.mappers.BigQueryMappers;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.BigQueryTableConfigManager;
import com.google.cloud.teleport.v2.transforms.BigQueryDynamicConverters;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.transforms.PubSubToFailSafeElement;
import com.google.cloud.teleport.v2.transforms.UDFTextTransformer.InputUDFOptions;
import com.google.cloud.teleport.v2.transforms.UDFTextTransformer.InputUDFToTableRow;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.cloud.teleport.v2.utils.ResourceUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link PubSubCdcToBigQuery} pipeline is a streaming pipeline which ingests data in JSON
 * format from Cloud Pub/Sub, executes an optional UDF, and outputs the resulting records to
 * BigQuery. Any errors which occur in the transformation of the data or execution of the UDF will
 * be output to a separate errors table in BigQuery. The errors table will be created if it does not
 * exist prior to execution. Both output and error tables are specified by the user as template
 * parameters.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The Pub/Sub topic and subscription exists.
 *   <li>The BigQuery output table exists or auto-mapping parameter is enabled.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT=project-id
 * BUCKET_NAME=gs://bucket-name
 * PIPELINE_FOLDER=${BUCKET_NAME}/dataflow/pipelines/pubsub-to-bigquery
 * SUBSCRIPTION=projects/${PROJECT}/subscriptions/pubsub-subscription-name
 * AUTOMAP_TABLES=false or true depedning if automatic new table/column handling is required.
 * DATASET_TEMPLATE=dataset-name
 * TABLE_NAME_TEMPLATE=table-name
 * DEADLETTER_TABLE=${PROJECT}:${DATASET_TEMPLATE}.dead_letter
 *
 * # Set containerization vars
 * IMAGE_NAME=pubsub-cdc-to-bigquery
 * TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
 * BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
 * BASE_CONTAINER_IMAGE_VERSION=latest
 * APP_ROOT=/template/pubsub-cdc-to-bigquery
 * DATAFLOW_JAVA_COMMAND_SPEC=${APP_ROOT}/resources/pubsub-cdc-to-bigquery-command-spec.json
 * TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/pubsub-cdc-to-bigquery-image-spec.json
 *
 * # Build and upload image
 * mvn clean package \
 * -Dimage=${TARGET_GCR_IMAGE} \
 * -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
 * -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
 * -Dapp-root=${APP_ROOT} \
 * -Dcommand-spec=${DATAFLOW_JAVA_COMMAND_SPEC} \
 * -am -pl pubsub-cdc-to-bigquery
 *
 * # Create a template spec containing the details of image location and metadata in GCS
 *   as specified in README.md file
 *
 * # Execute template:
 * JOB_NAME="pubsub-cdc-to-bigquery-`date +%Y%m%d-%H%M%S-%N`"
 * gcloud beta dataflow flex-template run ${JOB_NAME} \
 *      --project=${PROJECT} --region=us-central1 \
 *      --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
 *      --parameters inputSubscription=${SUBSCRIPTION},outputDatasetTemplate=${DATASET_TEMPLATE},outputTableNameTemplate=${TABLE_NAME_TEMPLATE},autoMapTables=${AUTOMAP_TABLES}
 * </pre>
 */
public class PubSubCdcToBigQuery {

  /** The log to output status messages to. */
  private static final Logger LOG = LoggerFactory.getLogger(PubSubCdcToBigQuery.class);

  /** The default suffix for error tables if dead letter table is not specified. */
  public static final String DEFAULT_DEADLETTER_TABLE_SUFFIX = "_error_records";

  /** Pubsub message/string coder for pipeline. */
  public static final FailsafeElementCoder<PubsubMessage, String> CODER =
      FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

  /** String/String Coder for FailsafeElement. */
  public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

  /**
   * The {@link Options} class provides the custom execution options passed by the executor at the
   * command-line.
   */
  public interface Options extends PipelineOptions, InputUDFOptions {
    @Description(
        "The Cloud Pub/Sub subscription to consume from. "
            + "The name should be in the format of "
            + "projects/<project-id>/subscriptions/<subscription-name>.")
    String getInputSubscription();

    void setInputSubscription(String value);

    @Description(
        "This determines if new columns and tables should be automatically created in BigQuery")
    @Default.Boolean(true)
    Boolean getAutoMapTables();

    void setAutoMapTables(Boolean value);

    @Description("GCS BigQuery schema fields file to be used in DDL")
    String getSchemaFilePath();

    void setSchemaFilePath(String value);

    @Description("The BigQuery Dataset Template")
    @Default.String("{_metadata_dataset}")
    String getOutputDatasetTemplate();

    void setOutputDatasetTemplate(String value);

    @Description("The BigQuery Table Template")
    @Default.String("_metadata_table")
    String getOutputTableNameTemplate();

    void setOutputTableNameTemplate(String value);

    @Description("DEPRECATED: Table spec to write the output to")
    String getOutputTableSpec();

    void setOutputTableSpec(String value);

    @Description(
        "The dead-letter table to output to within BigQuery in <project-id>:<dataset>.<table> "
            + "format. If it doesn't exist, it will be created during pipeline execution.")
    String getOutputDeadletterTable();

    void setOutputDeadletterTable(String value);

    // Dead Letter Queue GCS Directory
    @Description("The Dead Letter Queue GCS Prefix to use for errored data")
    @Default.String("")
    String getDeadLetterQueueDirectory();

    void setDeadLetterQueueDirectory(String value);

    // Window Duration
    @Description("The window duration for DLQ files")
    @Default.String("5s")
    String getWindowDuration();

    void setWindowDuration(String value);

    // Thread Count
    @Description("The number of threads to spawn")
    @Default.Integer(100)
    Integer getThreadCount();

    void setThreadCount(Integer value);
  }

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * PubSubCdcToBigQuery#run(Options)} method to start the pipeline and invoke {@code
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
    DeadLetterQueueManager dlqManager = buildDlqManager(options);
    String gcsOutputDateTimeDirectory = null;

    if (options.getDeadLetterQueueDirectory() != null) {
      gcsOutputDateTimeDirectory = dlqManager.getRetryDlqDirectory() + "YYYY/MM/DD/HH/mm/";
    }

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(CODER.getEncodedTypeDescriptor(), CODER);
    coderRegistry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

    InputUDFToTableRow<String> failsafeTableRowTransformer =
        new InputUDFToTableRow<String>(
            options.getJavascriptTextTransformGcsPath(),
            options.getJavascriptTextTransformFunctionName(),
            options.getPythonTextTransformGcsPath(),
            options.getPythonTextTransformFunctionName(),
            options.getRuntimeRetries(),
            FAILSAFE_ELEMENT_CODER);

    BigQueryTableConfigManager bqConfigManager =
        new BigQueryTableConfigManager(
            (String) options.as(GcpOptions.class).getProject(),
            (String) options.getOutputDatasetTemplate(),
            (String) options.getOutputTableNameTemplate(),
            (String) options.getOutputTableSpec());

    /*
     * Steps:
     *  1) Read messages in from Pub/Sub
     *  2) Transform the PubsubMessages into TableRows
     *     - Transform message payload via UDF
     *     - Convert UDF result to TableRow objects
     *  3) Write successful records out to BigQuery
     *     - Automap new objects to BigQuery if enabled
     *     - Write records to BigQuery tables
     *  4) Write failed records out to BigQuery
     */

    /*
     * Step #1: Read messages in from Pub/Sub
     */

    PCollection<PubsubMessage> messages =
        pipeline.apply(
            "ReadPubSubSubscription",
            PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputSubscription()));

    PCollection<FailsafeElement<String, String>> jsonRecords;

    if (options.getDeadLetterQueueDirectory() != null) {

      PCollection<FailsafeElement<String, String>> failsafeMessages =
          messages.apply("ConvertPubSubToFailsafe", ParDo.of(new PubSubToFailSafeElement()));

      PCollection<FailsafeElement<String, String>> dlqJsonRecords =
          pipeline
              .apply(dlqManager.dlqReconsumer())
              .apply(
                  ParDo.of(
                      new DoFn<String, FailsafeElement<String, String>>() {
                        @ProcessElement
                        public void process(
                            @Element String input,
                            OutputReceiver<FailsafeElement<String, String>> receiver) {
                          receiver.output(FailsafeElement.of(input, input));
                        }
                      }))
              .setCoder(FAILSAFE_ELEMENT_CODER);

      jsonRecords =
          PCollectionList.of(failsafeMessages).and(dlqJsonRecords).apply(Flatten.pCollections());
    } else {
      jsonRecords =
          messages.apply("ConvertPubSubToFailsafe", ParDo.of(new PubSubToFailSafeElement()));
    }

    PCollectionTuple convertedTableRows =
        jsonRecords
            /*
             * Step #2: Transform the PubsubMessages into TableRows
             */
            .apply(
                Reshuffle.<FailsafeElement<String, String>>viaRandomKey()
                    .withNumBuckets(options.getThreadCount()))
            .apply("ApplyUdfAndConvertToTableRow", failsafeTableRowTransformer);

    /*
     * Step #3: Write the successful records out to BigQuery
     *   Either extract table destination only
     *   or extract table destination and auto-map new columns
     */
    PCollection<KV<TableId, TableRow>> tableEvents;
    if (options.getAutoMapTables()) {
      tableEvents =
          convertedTableRows
              .get(failsafeTableRowTransformer.transformOut)
              .apply(
                  "Map Data to BigQuery Tables",
                  new BigQueryMappers(bqConfigManager.getProjectId())
                      .buildBigQueryTableMapper(
                          bqConfigManager.getDatasetTemplate(), bqConfigManager.getTableTemplate())
                      .withDefaultSchemaFromGCS(options.getSchemaFilePath()));

    } else {
      tableEvents =
          convertedTableRows
              .get(failsafeTableRowTransformer.transformOut)
              .apply(
                  "ExtractBigQueryTableDestination",
                  BigQueryDynamicConverters.extractTableRowDestination(
                      bqConfigManager.getProjectId(),
                      bqConfigManager.getDatasetTemplate(),
                      bqConfigManager.getTableTemplate()));
    }

    /*
     * Step #3: Cont.
     *    - Write rows out to BigQuery
     */
    // TODO(https://github.com/apache/beam/pull/12004): Switch out alwaysRetry
    WriteResult writeResult =
        tableEvents.apply(
            "WriteSuccessfulRecords",
            BigQueryIO.<KV<TableId, TableRow>>write()
                .to(new BigQueryDynamicConverters().bigQueryDynamicDestination())
                .withFormatFunction(element -> element.getValue())
                .withoutValidation()
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withExtendedErrorInfo()
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .withFailedInsertRetryPolicy(InsertRetryPolicy.alwaysRetry()));

    /*
     * Step 3 Contd.
     * Elements that failed inserts into BigQuery are extracted and converted to FailsafeElement
     */
    /*
     * Stage 4: Write Failures to GCS Dead Letter Queue
     */
    // TODO: Cover tableRowRecords.get(TRANSFORM_DEADLETTER_OUT) error values
    if (options.getDeadLetterQueueDirectory() != null) {

      writeResult
          .getFailedInsertsWithErr()
          .apply(
              "DLQ: Write Insert Failures to GCS",
              MapElements.via(new BigQueryDeadLetterQueueSanitizer()))
          .apply(
              "Creating " + options.getWindowDuration() + " Window",
              Window.into(
                  FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))
          .apply(
              "DLQ: Write File(s)",
              TextIO.write()
                  .withWindowedWrites()
                  .withNumShards(20)
                  .to(
                      new WindowedFilenamePolicy(
                          gcsOutputDateTimeDirectory, "error", "-SSSSS-of-NNNNN", ".json"))
                  .withTempDirectory(
                      FileBasedSink.convertToFileResourceIfPossible(
                          options.getDeadLetterQueueDirectory())));

      PCollection<FailsafeElement<String, String>> transformDeadletter =
          PCollectionList.of(
                  ImmutableList.of(
                      convertedTableRows.get(failsafeTableRowTransformer.udfDeadletterOut),
                      convertedTableRows.get(failsafeTableRowTransformer.transformDeadletterOut)))
              .apply("Flatten", Flatten.pCollections())
              .apply(
                  "Creating " + options.getWindowDuration() + " Window",
                  Window.into(
                      FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))));

      PCollection<String> dlqWindowing =
          transformDeadletter
              .apply("Sanitize records", MapElements.via(new StringDeadLetterQueueSanitizer()))
              .setCoder(StringUtf8Coder.of());

      dlqWindowing.apply(
          "DLQ: Write File(s)",
          TextIO.write()
              .withWindowedWrites()
              .withNumShards(20)
              .to(
                  new WindowedFilenamePolicy(
                      gcsOutputDateTimeDirectory, "error", "-SSSSS-of-NNNNN", ".json"))
              .withTempDirectory(
                  FileBasedSink.convertToFileResourceIfPossible(
                      gcsOutputDateTimeDirectory + "tmp/")));

    } else {
      PCollection<FailsafeElement<String, String>> failedInserts =
          writeResult
              .getFailedInsertsWithErr()
              .apply(
                  "WrapInsertionErrors",
                  MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                      .via(
                          (BigQueryInsertError e) -> BigQueryConverters.wrapBigQueryInsertError(e)))
              .setCoder(FAILSAFE_ELEMENT_CODER);

      /*
       * Step #4: Write records that failed table row transformation
       * or conversion out to BigQuery deadletter table.
       */
      PCollectionList.of(
              ImmutableList.of(
                  convertedTableRows.get(failsafeTableRowTransformer.udfDeadletterOut),
                  convertedTableRows.get(failsafeTableRowTransformer.transformDeadletterOut)))
          .apply("Flatten", Flatten.pCollections())
          .apply(
              "WriteFailedRecords",
              ErrorConverters.WriteStringMessageErrors.newBuilder()
                  .setErrorRecordsTable(
                      BigQueryConverters.maybeUseDefaultDeadletterTable(
                          options.getOutputDeadletterTable(),
                          bqConfigManager.getOutputTableSpec(),
                          DEFAULT_DEADLETTER_TABLE_SUFFIX))
                  .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                  .build());

      // 5) Insert records that failed insert into deadletter table
      failedInserts.apply(
          "WriteFailedRecords",
          ErrorConverters.WriteStringMessageErrors.newBuilder()
              .setErrorRecordsTable(
                  BigQueryConverters.maybeUseDefaultDeadletterTable(
                      options.getOutputDeadletterTable(),
                      bqConfigManager.getOutputTableSpec(),
                      DEFAULT_DEADLETTER_TABLE_SUFFIX))
              .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
              .build());
    }

    return pipeline.run();
  }

  /**
   * The {@link PubsubMessageToTableRow} class is a {@link PTransform} which transforms incoming
   * {@link PubsubMessage} objects into {@link TableRow} objects for insertion into BigQuery while
   * applying an optional UDF to the input. The executions of the UDF and transformation to {@link
   * TableRow} objects is done in a fail-safe way by wrapping the element with it's original payload
   * inside the {@link FailsafeElement} class. The {@link PubsubMessageToTableRow} transform will
   * output a {@link PCollectionTuple} which contains all output and dead-letter {@link
   * PCollection}.
   *
   * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
   *
   * <ul>
   *   <li>{@link PubSubCdcToBigQuery#UDF_OUT} - Contains all {@link FailsafeElement} records
   *       successfully processed by the optional UDF.
   *   <li>{@link PubSubCdcToBigQuery#UDF_DEADLETTER_OUT} - Contains all {@link FailsafeElement}
   *       records which failed processing during the UDF execution.
   *   <li>{@link PubSubCdcToBigQuery#TRANSFORM_OUT} - Contains all records successfully converted
   *       from JSON to {@link TableRow} objects.
   *   <li>{@link PubSubCdcToBigQuery#TRANSFORM_DEADLETTER_OUT} - Contains all {@link
   *       FailsafeElement} records which couldn't be converted to table rows.
   * </ul>
   */
  private static DeadLetterQueueManager buildDlqManager(Options options) {
    if (options.getDeadLetterQueueDirectory() != null) {
      String tempLocation =
          options.as(PipelineOptions.class).getTempLocation().endsWith("/")
              ? options.as(PipelineOptions.class).getTempLocation()
              : options.as(PipelineOptions.class).getTempLocation() + "/";

      String dlqDirectory =
          options.getDeadLetterQueueDirectory().isEmpty()
              ? tempLocation + "dlq/"
              : options.getDeadLetterQueueDirectory();

      return DeadLetterQueueManager.create(dlqDirectory);
    } else {
      return null;
    }
  }
}
