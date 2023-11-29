/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.Timestamp;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.cdc.dlq.StringDeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.SpannerChangeStreamsToBigQueryOptions;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.Mod;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.BigQueryUtils;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.OptionsUtils;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO(haikuo-google): Add integration test.
// TODO(haikuo-google): Add README.
// TODO(haikuo-google): Add stackdriver metrics.
// TODO(haikuo-google): Ideally side input should be used to store schema information and shared
// accross DoFns, but since side input fix is not yet deployed at the moment, we read schema
// information in the beginning of the DoFn as a work around. We should use side input instead when
// it's available.
// TODO(haikuo-google): Test the case where tables or columns are added while the pipeline is
// running.
/**
 * This pipeline ingests {@link DataChangeRecord} from Spanner change stream. The {@link
 * DataChangeRecord} is then broken into {@link Mod}, which converted into {@link TableRow} and
 * inserted into BigQuery table.
 */
@Template(
    name = "Spanner_Change_Streams_to_BigQuery",
    category = TemplateCategory.STREAMING,
    displayName = "Cloud Spanner change streams to BigQuery",
    description = {
      "The Cloud Spanner change streams to BigQuery template is a streaming pipeline that streams"
          + " Cloud Spanner data change records and writes them into BigQuery tables using Dataflow"
          + " Runner V2.\n",
      "All change stream watched columns are included in each BigQuery table row, regardless of"
          + " whether they are modified by a Cloud Spanner transaction. Columns not watched are not"
          + " included in the BigQuery row. Any Cloud Spanner change less than the Dataflow"
          + " watermark are either successfully applied to the BigQuery tables or are stored in the"
          + " dead-letter queue for retry. BigQuery rows are inserted out of order compared to the"
          + " original Cloud Spanner commit timestamp ordering.\n",
      "If the necessary BigQuery tables don't exist, the pipeline creates them. Otherwise, existing"
          + " BigQuery tables are used. The schema of existing BigQuery tables must contain the"
          + " corresponding tracked columns of the Cloud Spanner tables and any additional metadata"
          + " columns that are not ignored explicitly by the ignoreFields option. See the"
          + " description of the metadata fields in the following list. Each new BigQuery row"
          + " includes all columns watched by the change stream from its corresponding row in your"
          + " Cloud Spanner table at the change record's timestamp.\n",
      "The following metadata fields are added to BigQuery tables. For more details about these"
          + " fields, see Data change records in \"Change streams partitions, records, and"
          + " queries.\"\n"
          + "- _metadata_spanner_mod_type: The modification type (insert, update, or delete) of the"
          + " Cloud Spanner transaction. Extracted from change stream data change record.\n"
          + "- _metadata_spanner_table_name: The Cloud Spanner table name. Note this field is not"
          + " the metadata table name of the connector.\n"
          + "- _metadata_spanner_commit_timestamp: The Spanner commit timestamp, which is the time"
          + " when a change is committed. Extracted from change stream data change record.\n"
          + "- _metadata_spanner_server_transaction_id: A globally unique string that represents"
          + " the Spanner transaction in which the change was committed. Only use this value in the"
          + " context of processing change stream records. It isn't correlated with the transaction"
          + " ID in Spanner's API. Extracted from change stream data change record.\n"
          + "- _metadata_spanner_record_sequence: The sequence number for the record within the"
          + " Spanner transaction. Sequence numbers are guaranteed to be unique and monotonically"
          + " increasing (but not necessarily contiguous) within a transaction. Extracted from"
          + " change stream data change record.\n"
          + "- _metadata_spanner_is_last_record_in_transaction_in_partition: Indicates whether the"
          + " record is the last record for a Spanner transaction in the current partition."
          + " Extracted from change stream data change record.\n"
          + "- _metadata_spanner_number_of_records_in_transaction: The number of data change"
          + " records that are part of the Spanner transaction across all change stream partitions."
          + " Extracted from change stream data change record.\n"
          + "- _metadata_spanner_number_of_partitions_in_transaction: The number of partitions that"
          + " return data change records for the Spanner transaction. Extracted from change stream"
          + " data change record.\n"
          + "- _metadata_big_query_commit_timestamp: The commit timestamp of when the row is"
          + " inserted into BigQuery.\n",
      "Notes:\n"
          + "- This template does not propagate schema changes from Cloud Spanner to BigQuery."
          + " Because performing a schema change in Cloud Spanner is likely going to break the"
          + " pipeline, you might need to recreate the pipeline after the schema change.\n"
          + "- For OLD_AND_NEW_VALUES and NEW_VALUES value capture types, when the data change"
          + " record contains an UPDATE change, the template needs to do a stale read to Cloud"
          + " Spanner at the commit timestamp of the data change record to retrieve the unchanged"
          + " but watched columns. Make sure to configure your database 'version_retention_period'"
          + " properly for the stale read. For the NEW_ROW value capture type, the template is more"
          + " efficient, because the data change record captures the full new row including columns"
          + " that are not updated in UPDATEs, and the template does not need to do a stale read.\n"
          + "- You can minimize network latency and network transport costs by running the Dataflow"
          + " job from the same region as your Cloud Spanner instance or BigQuery tables. If you"
          + " use sources, sinks, staging file locations, or temporary file locations that are"
          + " located outside of your job's region, your data might be sent across regions. See"
          + " more about Dataflow regional endpoints.\n"
          + "- This template supports all valid Cloud Spanner data types, but if the BigQuery type"
          + " is more precise than the Cloud Spanner type, precision loss might occur during the"
          + " transformation. Specifically:\n"
          + "  - For Cloud Spanner JSON type, the order of the members of an object is"
          + " lexicographically ordered, but there is no such guarantee for BigQuery JSON type.\n"
          + "  - Cloud Spanner supports nanoseconds TIMESTAMP type, BigQuery only supports"
          + " microseconds TIMESTAMP type.\n",
      "Learn more about <a href=\"https://cloud.google.com/spanner/docs/change-streams\">change"
          + " streams</a>, <a"
          + " href=\"https://cloud.google.com/spanner/docs/change-streams/use-dataflow\">how to"
          + " build change streams Dataflow pipelines</a>, and <a"
          + " href=\"https://cloud.google.com/spanner/docs/change-streams/use-dataflow#best_practices\">best"
          + " practices</a>."
    },
    optionsClass = SpannerChangeStreamsToBigQueryOptions.class,
    flexContainerName = "spanner-changestreams-to-bigquery",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-spanner-change-streams-to-bigquery",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Cloud Spanner instance must exist prior to running the pipeline.",
      "The Cloud Spanner database must exist prior to running the pipeline.",
      "The Cloud Spanner metadata instance must exist prior to running the pipeline.",
      "The Cloud Spanner metadata database must exist prior to running the pipeline.",
      "The Cloud Spanner change stream must exist prior to running the pipeline.",
      "The BigQuery dataset must exist prior to running the pipeline."
    })
public final class SpannerChangeStreamsToBigQuery {

  /** String/String Coder for {@link FailsafeElement}. */
  public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

  private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeStreamsToBigQuery.class);

  // Max number of deadletter queue retries.
  private static final int DLQ_MAX_RETRIES = 5;

  private static final String USE_RUNNER_V2_EXPERIMENT = "use_runner_v2";

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    LOG.info("Starting to replicate change records from Spanner change streams to BigQuery");

    SpannerChangeStreamsToBigQueryOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(SpannerChangeStreamsToBigQueryOptions.class);

    run(options);
  }

  private static void validateOptions(SpannerChangeStreamsToBigQueryOptions options) {
    if (options.getDlqRetryMinutes() <= 0) {
      throw new IllegalArgumentException("dlqRetryMinutes must be positive.");
    }

    BigQueryIOUtils.validateBQStorageApiOptionsStreaming(options);
  }

  private static void setOptions(SpannerChangeStreamsToBigQueryOptions options) {
    options.setStreaming(true);
    options.setEnableStreamingEngine(true);

    // Add use_runner_v2 to the experiments option, since change streams connector is only supported
    // on Dataflow runner v2.
    List<String> experiments = options.getExperiments();
    if (experiments == null) {
      experiments = new ArrayList<>();
    }
    if (!experiments.contains(USE_RUNNER_V2_EXPERIMENT)) {
      experiments.add(USE_RUNNER_V2_EXPERIMENT);
    }
    options.setExperiments(experiments);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(SpannerChangeStreamsToBigQueryOptions options) {
    setOptions(options);
    validateOptions(options);

    /**
     * Stages: 1) Read {@link DataChangeRecord} from change stream. 2) Create {@link
     * FailsafeElement} of {@link Mod} JSON and merge from: - {@link DataChangeRecord}. - GCS Dead
     * letter queue. 3) Convert {@link Mod} JSON into {@link TableRow} by reading from Spanner at
     * commit timestamp. 4) Append {@link TableRow} to BigQuery. 5) Write Failures from 2), 3) and
     * 4) to GCS dead letter queue.
     */
    Pipeline pipeline = Pipeline.create(options);
    DeadLetterQueueManager dlqManager = buildDlqManager(options);
    String spannerProjectId = OptionsUtils.getSpannerProjectId(options);

    String dlqDirectory = dlqManager.getRetryDlqDirectoryWithDateTime();
    String tempDlqDirectory = dlqManager.getRetryDlqDirectory() + "tmp/";

    /**
     * There are two types of errors that can occur in this pipeline:
     *
     * <p>1) Error originating from modJsonStringToTableRow. Errors here are either due to pk values
     * missing, a spanner table / column missing in the in-memory map, or some Spanner read error
     * happening in readSpannerRow. We already retry the Spanner read error inline 3 times. Th other
     * types of errors are more likely to be un-retriable.
     *
     * <p>2) Error originating from BigQueryIO.write. BigQuery storage write API already retries all
     * transient errors and outputs more permanent errors.
     *
     * <p>As a result, it is reasonable to write all errors happening in the pipeline directly into
     * the permanent DLQ, since most of the errors are likely to be non-transient.
     */
    if (options.getDisableDlqRetries()) {
      LOG.info(
          "Disabling retries for the DLQ, directly writing into severe DLQ: {}",
          dlqManager.getSevereDlqDirectoryWithDateTime());
      dlqDirectory = dlqManager.getSevereDlqDirectoryWithDateTime();
      tempDlqDirectory = dlqManager.getSevereDlqDirectory() + "tmp/";
    }

    // Retrieve and parse the startTimestamp and endTimestamp.
    Timestamp startTimestamp =
        options.getStartTimestamp().isEmpty()
            ? Timestamp.now()
            : Timestamp.parseTimestamp(options.getStartTimestamp());
    Timestamp endTimestamp =
        options.getEndTimestamp().isEmpty()
            ? Timestamp.MAX_VALUE
            : Timestamp.parseTimestamp(options.getEndTimestamp());

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withHost(ValueProvider.StaticValueProvider.of(options.getSpannerHost()))
            .withProjectId(spannerProjectId)
            .withInstanceId(options.getSpannerInstanceId())
            .withDatabaseId(options.getSpannerDatabase())
            .withRpcPriority(options.getRpcPriority());
    // Propagate database role for fine-grained access control on change stream.
    if (options.getSpannerDatabaseRole() != null) {
      spannerConfig =
          spannerConfig.withDatabaseRole(
              ValueProvider.StaticValueProvider.of(options.getSpannerDatabaseRole()));
    }

    SpannerIO.ReadChangeStream readChangeStream =
        SpannerIO.readChangeStream()
            .withSpannerConfig(spannerConfig)
            .withMetadataInstance(options.getSpannerMetadataInstanceId())
            .withMetadataDatabase(options.getSpannerMetadataDatabase())
            .withChangeStreamName(options.getSpannerChangeStreamName())
            .withInclusiveStartAt(startTimestamp)
            .withInclusiveEndAt(endTimestamp)
            .withRpcPriority(options.getRpcPriority());

    String spannerMetadataTableName = options.getSpannerMetadataTableName();
    if (spannerMetadataTableName != null) {
      readChangeStream = readChangeStream.withMetadataTable(spannerMetadataTableName);
    }

    PCollection<DataChangeRecord> dataChangeRecord =
        pipeline
            .apply("Read from Spanner Change Streams", readChangeStream)
            .apply("Reshuffle DataChangeRecord", Reshuffle.viaRandomKey());

    PCollection<FailsafeElement<String, String>> sourceFailsafeModJson =
        dataChangeRecord
            .apply("DataChangeRecord To Mod JSON", ParDo.of(new DataChangeRecordToModJsonFn()))
            .apply(
                "Wrap Mod JSON In FailsafeElement",
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

    PCollectionTuple dlqModJson =
        dlqManager.getReconsumerDataTransform(
            pipeline.apply(dlqManager.dlqReconsumer(options.getDlqRetryMinutes())));
    PCollection<FailsafeElement<String, String>> retryableDlqFailsafeModJson =
        dlqModJson.get(DeadLetterQueueManager.RETRYABLE_ERRORS).setCoder(FAILSAFE_ELEMENT_CODER);

    PCollection<FailsafeElement<String, String>> failsafeModJson =
        PCollectionList.of(sourceFailsafeModJson)
            .and(retryableDlqFailsafeModJson)
            .apply("Merge Source And DLQ Mod JSON", Flatten.pCollections());

    ImmutableSet.Builder<String> ignoreFieldsBuilder = ImmutableSet.builder();
    for (String ignoreField : options.getIgnoreFields().split(",")) {
      ignoreFieldsBuilder.add(ignoreField);
    }
    ImmutableSet<String> ignoreFields = ignoreFieldsBuilder.build();
    FailsafeModJsonToTableRowTransformer.FailsafeModJsonToTableRowOptions
        failsafeModJsonToTableRowOptions =
            FailsafeModJsonToTableRowTransformer.FailsafeModJsonToTableRowOptions.builder()
                .setSpannerConfig(spannerConfig)
                .setSpannerChangeStream(options.getSpannerChangeStreamName())
                .setIgnoreFields(ignoreFields)
                .setCoder(FAILSAFE_ELEMENT_CODER)
                .setUseStorageWriteApi(options.getUseStorageWriteApi())
                .build();
    FailsafeModJsonToTableRowTransformer.FailsafeModJsonToTableRow failsafeModJsonToTableRow =
        new FailsafeModJsonToTableRowTransformer.FailsafeModJsonToTableRow(
            failsafeModJsonToTableRowOptions);

    PCollectionTuple tableRowTuple =
        failsafeModJson.apply("Mod JSON To TableRow", failsafeModJsonToTableRow);
    // If users pass in the full BigQuery dataset ID (projectId.datasetName), extract the dataset
    // name for the setBigQueryDataset parameter.
    List<String> results = OptionsUtils.processBigQueryProjectAndDataset(options);
    String bigqueryProject = results.get(0);
    String bigqueryDataset = results.get(1);

    BigQueryDynamicDestinations.BigQueryDynamicDestinationsOptions
        bigQueryDynamicDestinationsOptions =
            BigQueryDynamicDestinations.BigQueryDynamicDestinationsOptions.builder()
                .setSpannerConfig(spannerConfig)
                .setChangeStreamName(options.getSpannerChangeStreamName())
                .setIgnoreFields(ignoreFields)
                .setBigQueryProject(bigqueryProject)
                .setBigQueryDataset(bigqueryDataset)
                .setBigQueryTableTemplate(options.getBigQueryChangelogTableNameTemplate())
                .setUseStorageWriteApi(options.getUseStorageWriteApi())
                .build();
    WriteResult writeResult =
        tableRowTuple
            .get(failsafeModJsonToTableRow.transformOut)
            .apply(
                "Write To BigQuery",
                BigQueryIO.<TableRow>write()
                    .to(BigQueryDynamicDestinations.of(bigQueryDynamicDestinationsOptions))
                    .withFormatFunction(element -> removeIntermediateMetadataFields(element))
                    .withFormatRecordOnFailureFunction(element -> element)
                    .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(Write.WriteDisposition.WRITE_APPEND)
                    .withExtendedErrorInfo()
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));

    PCollection<String> transformDlqJson =
        tableRowTuple
            .get(failsafeModJsonToTableRow.transformDeadLetterOut)
            .apply(
                "Failed Mod JSON During Table Row Transformation",
                MapElements.via(new StringDeadLetterQueueSanitizer()));

    PCollection<String> bqWriteDlqJson =
        BigQueryIOUtils.writeResultToBigQueryInsertErrors(writeResult, options)
            .apply(
                "Failed Mod JSON During BigQuery Writes",
                MapElements.via(new BigQueryDeadLetterQueueSanitizer()));

    PCollectionList.of(transformDlqJson)
        // Generally BigQueryIO storage write retries transient errors, and only more
        // persistent errors make it into DLQ.
        .and(bqWriteDlqJson)
        .apply("Merge Failed Mod JSON From Transform And BigQuery", Flatten.pCollections())
        .apply(
            "Write Failed Mod JSON To DLQ",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqDirectory)
                .withTmpDirectory(tempDlqDirectory)
                .setIncludePaneInfo(true)
                .build());

    PCollection<FailsafeElement<String, String>> nonRetryableDlqModJsonFailsafe =
        dlqModJson.get(DeadLetterQueueManager.PERMANENT_ERRORS).setCoder(FAILSAFE_ELEMENT_CODER);

    nonRetryableDlqModJsonFailsafe
        .apply(
            "Write Mod JSON With Non-retryable Error To DLQ",
            MapElements.via(new StringDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqManager.getSevereDlqDirectoryWithDateTime())
                .withTmpDirectory(dlqManager.getSevereDlqDirectory() + "tmp/")
                .setIncludePaneInfo(true)
                .build());

    return pipeline.run();
  }

  private static DeadLetterQueueManager buildDlqManager(
      SpannerChangeStreamsToBigQueryOptions options) {
    String tempLocation =
        options.as(DataflowPipelineOptions.class).getTempLocation().endsWith("/")
            ? options.as(DataflowPipelineOptions.class).getTempLocation()
            : options.as(DataflowPipelineOptions.class).getTempLocation() + "/";
    String dlqDirectory =
        options.getDeadLetterQueueDirectory().isEmpty()
            ? tempLocation + "dlq/"
            : options.getDeadLetterQueueDirectory();

    LOG.info("Dead letter queue directory: {}", dlqDirectory);
    return DeadLetterQueueManager.create(dlqDirectory, DLQ_MAX_RETRIES);
  }

  /**
   * Remove the following intermediate metadata fields that are not user data from {@link TableRow}:
   * _metadata_error, _metadata_retry_count, _metadata_spanner_original_payload_json.
   */
  private static TableRow removeIntermediateMetadataFields(TableRow tableRow) {
    TableRow cleanTableRow = tableRow.clone();
    Set<String> rowKeys = tableRow.keySet();
    Set<String> metadataFields = BigQueryUtils.getBigQueryIntermediateMetadataFieldNames();

    for (String rowKey : rowKeys) {
      if (metadataFields.contains(rowKey)) {
        cleanTableRow.remove(rowKey);
      }
    }

    return cleanTableRow;
  }

  /**
   * DoFn that converts a {@link DataChangeRecord} to multiple {@link Mod} in serialized JSON
   * format.
   */
  static class DataChangeRecordToModJsonFn extends DoFn<DataChangeRecord, String> {

    @ProcessElement
    public void process(@Element DataChangeRecord input, OutputReceiver<String> receiver) {
      for (org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod changeStreamsMod :
          input.getMods()) {
        Mod mod =
            new Mod(
                changeStreamsMod.getKeysJson(),
                changeStreamsMod.getNewValuesJson(),
                input.getCommitTimestamp(),
                input.getServerTransactionId(),
                input.isLastRecordInTransactionInPartition(),
                input.getRecordSequence(),
                input.getTableName(),
                input.getModType(),
                input.getValueCaptureType(),
                input.getNumberOfRecordsInTransaction(),
                input.getNumberOfPartitionsInTransaction());

        String modJsonString;

        try {
          modJsonString = mod.toJson();
        } catch (IOException e) {
          // Ignore exception and print bad format.
          modJsonString = String.format("\"%s\"", input);
        }
        receiver.output(modJsonString);
      }
    }
  }
}
