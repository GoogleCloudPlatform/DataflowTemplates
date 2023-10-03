/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions.ReadChangeStreamOptions;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions.ReadOptions;
import com.google.cloud.teleport.v2.bigtable.utils.UnsupportedEntryException;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.options.BigtableChangeStreamToBigQueryOptions;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.BigQueryDestination;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.BigtableSource;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.Mod;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.ModType;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.schemautils.BigQueryUtils;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.ExistingPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline ingests {@link ChangeStreamMutation} from Bigtable change stream. The {@link
 * ChangeStreamMutation} is then broken into {@link Mod}, which converted into {@link TableRow} and
 * inserted into BigQuery table.
 */
@Template(
    name = "Bigtable_Change_Streams_to_BigQuery",
    category = TemplateCategory.STREAMING,
    displayName = "Cloud Bigtable Change Streams to BigQuery",
    description =
        "Streaming pipeline. Streams Bigtable data change records and writes them into BigQuery using Dataflow Runner V2.",
    optionsClass = BigtableChangeStreamToBigQueryOptions.class,
    optionsOrder = {
      BigtableChangeStreamToBigQueryOptions.class,
      ReadChangeStreamOptions.class,
      ReadOptions.class
    },
    skipOptions = {
      "bigtableReadAppProfile",
      "bigtableAdditionalRetryCodes",
      "bigtableRpcAttemptTimeoutMs",
      "bigtableRpcTimeoutMs"
    },
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-bigtable-change-streams-to-bigquery",
    flexContainerName = "bigtable-changestreams-to-bigquery",
    contactInformation = "https://cloud.google.com/support")
public final class BigtableChangeStreamsToBigQuery {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableChangeStreamsToBigQuery.class);

  private static final String USE_RUNNER_V2_EXPERIMENT = "use_runner_v2";

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    LOG.info("Starting to replicate change records from Cloud Bigtable change streams to BigQuery");

    BigtableChangeStreamToBigQueryOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BigtableChangeStreamToBigQueryOptions.class);

    run(options);
  }

  private static void setOptions(BigtableChangeStreamToBigQueryOptions options) {
    options.setStreaming(true);
    options.setEnableStreamingEngine(true);

    // Add use_runner_v2 to the experiments option, since change streams connector is only supported
    // on Dataflow runner v2.
    List<String> experiments = options.getExperiments();
    if (experiments == null) {
      experiments = new ArrayList<>();
    }
    boolean hasUseRunnerV2 = false;
    for (String experiment : experiments) {
      if (experiment.equalsIgnoreCase(USE_RUNNER_V2_EXPERIMENT)) {
        hasUseRunnerV2 = true;
        break;
      }
    }
    if (!hasUseRunnerV2) {
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
  public static PipelineResult run(BigtableChangeStreamToBigQueryOptions options) {
    setOptions(options);

    String changelogTableName = getBigQueryChangelogTableName(options);
    String bigtableProject = getBigtableProjectId(options);
    String bigQueryProject = getBigQueryProjectId(options);
    String bigQueryDataset = options.getBigQueryDataset();

    // If dataset doesn't exist and not checked, pipeline will start failing only after it sees the
    // first change from Cloud Bigtable. BigQueryIO can create table if it doesn't exist, but it
    // cannot create a dataset
    validateBigQueryDatasetExists(bigQueryProject, bigQueryDataset);

    // Retrieve and parse the startTimestamp
    Instant startTimestamp =
        options.getBigtableChangeStreamStartTimestamp().isEmpty()
            ? Instant.now()
            : toInstant(Timestamp.parseTimestamp(options.getBigtableChangeStreamStartTimestamp()));

    BigtableSource sourceInfo =
        new BigtableSource(
            options.getBigtableReadInstanceId(),
            options.getBigtableReadTableId(),
            getBigtableCharset(options),
            options.getBigtableChangeStreamIgnoreColumnFamilies(),
            options.getBigtableChangeStreamIgnoreColumns(),
            startTimestamp);

    BigQueryDestination destinationInfo =
        new BigQueryDestination(
            bigQueryProject,
            bigQueryDataset,
            changelogTableName,
            options.getWriteRowkeyAsBytes(),
            options.getWriteValuesAsBytes(),
            options.getWriteNumericTimestamps(),
            options.getBigQueryChangelogTablePartitionGranularity(),
            options.getBigQueryChangelogTablePartitionExpirationMs(),
            options.getBigQueryChangelogTableFieldsToIgnore());

    BigQueryUtils bigQuery = new BigQueryUtils(sourceInfo, destinationInfo);

    Pipeline pipeline = Pipeline.create(options);
    DeadLetterQueueManager dlqManager = buildDlqManager(options);

    BigtableIO.ReadChangeStream readChangeStream =
        BigtableIO.readChangeStream()
            .withChangeStreamName(options.getBigtableChangeStreamName())
            .withExistingPipelineOptions(
                options.getBigtableChangeStreamResume()
                    ? ExistingPipelineOptions.RESUME_OR_FAIL
                    : ExistingPipelineOptions.FAIL_IF_EXISTS)
            .withProjectId(bigtableProject)
            .withMetadataTableInstanceId(options.getBigtableChangeStreamMetadataInstanceId())
            .withInstanceId(options.getBigtableReadInstanceId())
            .withTableId(options.getBigtableReadTableId())
            .withAppProfileId(options.getBigtableChangeStreamAppProfile())
            .withStartTime(startTimestamp);

    if (!StringUtils.isBlank(options.getBigtableChangeStreamMetadataTableTableId())) {
      readChangeStream =
          readChangeStream.withMetadataTableTableId(
              options.getBigtableChangeStreamMetadataTableTableId());
    }

    PCollection<ChangeStreamMutation> dataChangeRecord =
        pipeline
            .apply("Read from Cloud Bigtable Change Streams", readChangeStream)
            .apply(Values.create());

    PCollection<TableRow> changeStreamMutationToTableRow =
        dataChangeRecord.apply(
            "ChangeStreamMutation To TableRow",
            ParDo.of(new ChangeStreamMutationToTableRowFn(sourceInfo, bigQuery)));

    Write<TableRow> bigQueryWrite =
        BigQueryIO.<TableRow>write()
            .to(destinationInfo.getBigQueryTableReference())
            .withSchema(bigQuery.getDestinationTableSchema())
            .withFormatFunction(element -> element)
            .withFormatRecordOnFailureFunction(element -> element)
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withExtendedErrorInfo()
            .withMethod(Write.Method.STORAGE_API_AT_LEAST_ONCE)
            .withNumStorageWriteApiStreams(0)
            .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors());

    if (destinationInfo.isPartitioned()) {
      bigQueryWrite = bigQueryWrite.withTimePartitioning(bigQuery.getTimePartitioning());
    }

    // Unfortunately, due to https://github.com/apache/beam/issues/24090, it is no longer possible
    // to pass metadata via fake columns when writing to BigQuery. Previously we'd pass something
    // like retry count and then format it out before writing, but BQ would return original object
    // which would allow us to increment retry count and store it to DLQ with incremented number.
    // Because WRITE API doesn't allow access to original object, all metadata values are stripped
    // and we can only rely on retry policy and put all other persistently failing rows to DLQ as
    // a non-retriable severe failure.
    //
    // Since we're not going to be retrying such failures, we'll not use any reading from DLQ
    // capability.

    WriteResult writeResult =
        changeStreamMutationToTableRow.apply("Write To BigQuery", bigQueryWrite);

    writeResult
        .getFailedStorageApiInserts()
        .apply(
            "Failed Mod JSON During BigQuery Writes",
            MapElements.via(new BigQueryDeadLetterQueueSanitizer()))
        .apply(
            "Write rejected TableRow JSON To DLQ",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqManager.getSevereDlqDirectory() + "YYYY/MM/dd/HH/mm/")
                .withTmpDirectory(dlqManager.getSevereDlqDirectory() + "tmp/")
                .setIncludePaneInfo(true)
                .build());

    return pipeline.run();
  }

  private static void validateBigQueryDatasetExists(
      String bigQueryProject, String bigQueryDataset) {
    BigQueryOptions options = BigQueryOptions.newBuilder().build();
    options.setThrowNotFound(true);

    BigQuery bigQuery = options.getService();
    bigQuery.getDataset(DatasetId.of(bigQueryProject, bigQueryDataset));
  }

  private static Instant toInstant(Timestamp timestamp) {
    if (timestamp == null) {
      return null;
    } else {
      return Instant.ofEpochMilli(timestamp.getSeconds() * 1000 + timestamp.getNanos() / 1000000);
    }
  }

  private static DeadLetterQueueManager buildDlqManager(
      BigtableChangeStreamToBigQueryOptions options) {
    String tempLocation =
        options.as(DataflowPipelineOptions.class).getTempLocation().endsWith("/")
            ? options.as(DataflowPipelineOptions.class).getTempLocation()
            : options.as(DataflowPipelineOptions.class).getTempLocation() + "/";
    String dlqDirectory =
        options.getDlqDirectory().isEmpty() ? tempLocation + "dlq/" : options.getDlqDirectory();

    LOG.info("Dead letter queue directory: {}", dlqDirectory);
    return DeadLetterQueueManager.create(dlqDirectory, 1);
  }

  private static String getBigtableCharset(BigtableChangeStreamToBigQueryOptions options) {
    return StringUtils.isEmpty(options.getBigtableChangeStreamCharset())
        ? "UTF-8"
        : options.getBigtableChangeStreamCharset();
  }

  private static String getBigtableProjectId(BigtableChangeStreamToBigQueryOptions options) {
    return StringUtils.isEmpty(options.getBigtableReadProjectId())
        ? options.getProject()
        : options.getBigtableReadProjectId();
  }

  private static String getBigQueryChangelogTableName(
      BigtableChangeStreamToBigQueryOptions options) {
    return StringUtils.isEmpty(options.getBigQueryChangelogTableName())
        ? options.getBigtableReadTableId() + "_changelog"
        : options.getBigQueryChangelogTableName();
  }

  private static String getBigQueryProjectId(BigtableChangeStreamToBigQueryOptions options) {
    return StringUtils.isEmpty(options.getBigQueryProjectId())
        ? options.getProject()
        : options.getBigQueryProjectId();
  }

  /**
   * DoFn that converts a {@link ChangeStreamMutation} to multiple {@link Mod} in serialized JSON
   * format.
   */
  static class ChangeStreamMutationToTableRowFn extends DoFn<ChangeStreamMutation, TableRow> {
    private final BigtableSource sourceInfo;
    private final BigQueryUtils bigQuery;

    ChangeStreamMutationToTableRowFn(BigtableSource source, BigQueryUtils bigQuery) {
      this.sourceInfo = source;
      this.bigQuery = bigQuery;
    }

    @ProcessElement
    public void process(@Element ChangeStreamMutation input, OutputReceiver<TableRow> receiver)
        throws Exception {
      for (Entry entry : input.getEntries()) {
        ModType modType = getModType(entry);

        Mod mod = null;
        switch (modType) {
          case SET_CELL:
            mod = new Mod(sourceInfo, input, (SetCell) entry);
            break;
          case DELETE_CELLS:
            mod = new Mod(sourceInfo, input, (DeleteCells) entry);
            break;
          case DELETE_FAMILY:
            mod = new Mod(sourceInfo, input, (DeleteFamily) entry);
            break;
          default:
          case UNKNOWN:
            throw new UnsupportedEntryException(
                "Cloud Bigtable change stream entry of type "
                    + entry.getClass().getName()
                    + " is not supported. The entry was put into a dead letter queue directory. "
                    + "Please update your Dataflow template with the latest template version");
        }

        TableRow tableRow = new TableRow();
        if (bigQuery.setTableRowFields(mod, tableRow)) {
          receiver.output(tableRow);
        }
      }
    }

    private ModType getModType(Entry entry) {
      if (entry instanceof SetCell) {
        return ModType.SET_CELL;
      } else if (entry instanceof DeleteCells) {
        return ModType.DELETE_CELLS;
      } else if (entry instanceof DeleteFamily) {
        return ModType.DELETE_FAMILY;
      }
      return ModType.UNKNOWN;
    }
  }
}
