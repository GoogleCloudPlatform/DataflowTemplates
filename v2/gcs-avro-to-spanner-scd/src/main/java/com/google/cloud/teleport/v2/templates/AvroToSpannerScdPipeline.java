/*
 * Copyright (C) 2024 Google LLC
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

import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateCreationParameter;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.v2.transforms.AvroToStructFn;
import com.google.cloud.teleport.v2.transforms.MakeBatchesTransform;
import com.google.cloud.teleport.v2.transforms.SpannerScdMutationTransform;
import com.google.cloud.teleport.v2.utils.CurrentTimestampGetter;
import com.google.cloud.teleport.v2.utils.SpannerFactory;
import com.google.cloud.teleport.v2.utils.SpannerFactory.DatabaseClientManager;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Represents an Apache Beam batch pipeline to insert data in Avro format into Spanner using the
 * requested SCD Type.
 *
 * <ul>
 *   <li>SCD Type 1: Insert new rows and update existing rows based on primary key.
 *   <li>SCD Type 2: Insert all rows. Mark existing rows as inactive by setting start and end dates.
 *   <li>All other SCD Types are not currently supported.
 * </ul>
 */
@Template(
    name = "gcs_avro_to_spanner_scd",
    category = TemplateCategory.BATCH,
    displayName = "GCS Avro to Spanner Using SCD",
    description =
        "Batch pipeline to insert data in Avro format into Spanner using the requested SCD Type.",
    flexContainerName = "gcs-avro-to-spanner-scd",
    optionsClass = AvroToSpannerScdPipeline.AvroToSpannerScdOptions.class,
    requirements = {
      "The Avro files must contain all table columns (other than the ones required for SCD).",
      "The Spanner tables must exist before pipeline execution.",
      "Spanner tables must have a compatible schema with the provided.",
      "The relational database must be accessible from the subnet where Dataflow runs.",
      "If using SCD Type 2, (start and) end date must be a TIMESTAMP.",
    })
public class AvroToSpannerScdPipeline {
  private final Pipeline pipeline;
  private final AvroToSpannerScdOptions pipelineOptions;
  private final SpannerConfig spannerConfig;
  private final SpannerFactory spannerFactory;

  private final CurrentTimestampGetter currentTimestampGetter;

  /**
   * Initializes the pipeline.
   *
   * @param pipeline the Apache Beam pipeline
   * @param pipelineOptions the Apache Beam pipeline options to configure the pipeline
   */
  @VisibleForTesting
  public AvroToSpannerScdPipeline(
      Pipeline pipeline,
      AvroToSpannerScdOptions pipelineOptions,
      SpannerConfig spannerConfig,
      SpannerFactory spannerFactory,
      CurrentTimestampGetter currentTimestampGetter) {
    this.pipeline = pipeline;
    this.pipelineOptions = pipelineOptions;
    this.spannerConfig = spannerConfig;
    this.spannerFactory = spannerFactory;
    this.currentTimestampGetter = currentTimestampGetter;
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    AvroToSpannerScdPipeline.AvroToSpannerScdOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(AvroToSpannerScdPipeline.AvroToSpannerScdOptions.class);

    PipelineResult result = run(options);

    if (options.getWaitUntilFinish()
        &&
        /* Only if template location is null, there is a dataflow job to wait for. Otherwise, it's
         * template generation, which doesn't start a dataflow job.
         */
        options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
      result.waitUntilFinish();
    }
  }

  @VisibleForTesting
  static void validateOptions(AvroToSpannerScdOptions pipelineOptions) {
    if (pipelineOptions.getSpannerProjectId() != null
        && pipelineOptions.getSpannerProjectId().equals("")) {
      throw new IllegalArgumentException("When provided, Spanner project id must not be empty.");
    }

    if (pipelineOptions.getInstanceId() == null || pipelineOptions.getInstanceId().equals("")) {
      throw new IllegalArgumentException("Spanner instance id must not be empty.");
    }

    if (pipelineOptions.getDatabaseId() == null || pipelineOptions.getDatabaseId().equals("")) {
      throw new IllegalArgumentException("Spanner database id must not be empty.");
    }

    if (pipelineOptions.getSpannerBatchSize() <= 0) {
      throw new IllegalArgumentException(
          String.format(
              "Batch size must be greater than 0. Provided: %s.",
              pipelineOptions.getSpannerBatchSize()));
    }

    if (pipelineOptions.getTableName() == null || pipelineOptions.getTableName().equals("")) {
      throw new IllegalArgumentException("Spanner table name must not be empty.");
    }

    if (pipelineOptions.getPrimaryKeyColumnNames().size() == 0) {
      throw new IllegalArgumentException("Spanner primary key column names must not be empty.");
    }

    if (pipelineOptions.getOrderByColumnName() != null
        && pipelineOptions.getOrderByColumnName().equals("")) {
      throw new IllegalArgumentException("When provided, order by column name must not be empty.");
    }

    switch (pipelineOptions.getScdType()) {
      case TYPE_1:
        if (pipelineOptions.getStartDateColumnName() != null) {
          throw new IllegalArgumentException(
              "When using SCD Type 1, start date column name is not used.");
        }

        if (pipelineOptions.getEndDateColumnName() != null) {
          throw new IllegalArgumentException(
              "When using SCD Type 1, end date column name is not used.");
        }

        break;

      case TYPE_2:
        if (pipelineOptions.getStartDateColumnName() != null
            && pipelineOptions.getStartDateColumnName().equals("")) {
          throw new IllegalArgumentException(
              "When provided, start date column name must not be empty.");
        }

        if (pipelineOptions.getEndDateColumnName() == null) {
          throw new IllegalArgumentException(
              "When using SCD Type 2, end date column name must be provided.");
        } else if (pipelineOptions.getEndDateColumnName().equals("")) {
          throw new IllegalArgumentException(
              "When using SCD Type 2, end date column name must not be empty.");
        }

        break;

      default:
        break;
    }
  }

  private static PipelineResult run(AvroToSpannerScdOptions pipelineOptions) {
    validateOptions(pipelineOptions);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(
                pipelineOptions.getSpannerProjectId() != null
                    ? pipelineOptions.getSpannerProjectId()
                    : SpannerOptions.getDefaultProjectId())
            .withHost(ValueProvider.StaticValueProvider.of(pipelineOptions.getSpannerHost()))
            .withInstanceId(pipelineOptions.getInstanceId())
            .withDatabaseId(pipelineOptions.getDatabaseId())
            .withRpcPriority(pipelineOptions.getSpannerPriority());

    SpannerFactory spannerFactory = SpannerFactory.withSpannerConfig(spannerConfig);
    CurrentTimestampGetter currentTimestampGetter = CurrentTimestampGetter.create();

    return new AvroToSpannerScdPipeline(
            Pipeline.create(pipelineOptions),
            pipelineOptions,
            spannerConfig,
            spannerFactory,
            currentTimestampGetter)
        .makePipeline()
        .run();
  }

  /**
   * Creates the Apache Beam pipeline that write data from Avro to Spanner using SCD Type 2.
   *
   * @return the pipeline to upsert data from GCS Avro to Spanner using the specified SCD Type.
   * @see org.apache.beam.sdk.Pipeline
   */
  private Pipeline makePipeline() {
    pipeline
        .apply(
            "ReadAvroRecordsAsStruct",
            AvroIO.parseGenericRecords(AvroToStructFn.create())
                .from(pipelineOptions.getInputFilePattern()))
        .apply(
            "BatchRowsIntoGroups",
            MakeBatchesTransform.builder()
                .setBatchSize(pipelineOptions.getSpannerBatchSize())
                .setPrimaryKeyColumns(pipelineOptions.getPrimaryKeyColumnNames())
                .setOrderByColumnName(pipelineOptions.getOrderByColumnName())
                .setEndDateColumnName(pipelineOptions.getEndDateColumnName())
                .setOrderByOrder(pipelineOptions.getOrderByOrder())
                .build())
        .apply(
            "WriteScdChangesToSpanner",
            SpannerScdMutationTransform.builder()
                .setScdType(pipelineOptions.getScdType())
                .setSpannerConfig(spannerConfig)
                .setTableName(pipelineOptions.getTableName())
                .setPrimaryKeyColumnNames(pipelineOptions.getPrimaryKeyColumnNames())
                .setStartDateColumnName(pipelineOptions.getStartDateColumnName())
                .setEndDateColumnName(pipelineOptions.getEndDateColumnName())
                .setTableColumnNames(
                    getTableColumnNames(spannerConfig, pipelineOptions.getTableName()))
                .setSpannerFactory(spannerFactory)
                .setCurrentTimestampGetter(currentTimestampGetter)
                .build());

    return pipeline;
  }

  private static Iterable<String> getTableColumnNames(
      SpannerConfig spannerConfig, String tableName) {
    DatabaseClientManager databaseClientManager =
        SpannerFactory.withSpannerConfig(spannerConfig).getDatabaseClientManager();

    String schemaQuery =
        String.format(
            "SELECT COLUMN_NAME FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE TABLE_NAME = \"%s\"",
            tableName);
    ResultSet results =
        databaseClientManager
            .getDatabaseClient()
            .readOnlyTransaction()
            .executeQuery(Statement.of(schemaQuery));

    ArrayList<String> columnNames = new ArrayList<>();
    while (results.next()) {
      Struct rowStruct = results.getCurrentRowAsStruct();
      columnNames.add(rowStruct.getString("COLUMN_NAME"));
    }

    databaseClientManager.close();
    return columnNames;
  }

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface AvroToSpannerScdOptions extends PipelineOptions {

    @TemplateParameter.Text(
        groupName = "Source",
        order = 1,
        description = "Cloud storage file pattern",
        helpText = "The Cloud Storage file pattern where the Avro files are imported from.")
    String getInputFilePattern();

    void setInputFilePattern(String value);

    @TemplateParameter.ProjectId(
        groupName = "Target",
        order = 2,
        optional = true,
        description = "Cloud Spanner project ID",
        helpText =
            "The ID of the Google Cloud project that contains the Spanner database. If not set, the"
                + " default Google Cloud project is used.")
    String getSpannerProjectId();

    void setSpannerProjectId(String value);

    @TemplateParameter.Text(
        groupName = "Target",
        order = 3,
        regexes = {"^[a-z0-9\\-]+$"},
        description = "Cloud Spanner instance ID",
        helpText = "The instance ID of the Spanner database.")
    String getInstanceId();

    void setInstanceId(String value);

    @TemplateParameter.Text(
        groupName = "Target",
        order = 4,
        regexes = {"^[a-z_0-9\\-]+$"},
        description = "Cloud Spanner database ID",
        helpText = "The database ID of the Spanner database.")
    String getDatabaseId();

    void setDatabaseId(String value);

    @TemplateParameter.Text(
        groupName = "Target",
        order = 5,
        optional = true,
        description = "Cloud Spanner endpoint to call",
        helpText = "The Cloud Spanner endpoint to call in the template. Only used for testing.",
        example = "https://batch-spanner.googleapis.com")
    @Default.String("https://batch-spanner.googleapis.com")
    String getSpannerHost();

    void setSpannerHost(String value);

    @TemplateParameter.Enum(
        groupName = "Target",
        order = 6,
        enumOptions = {
          @TemplateEnumOption("LOW"),
          @TemplateEnumOption("MEDIUM"),
          @TemplateEnumOption("HIGH")
        },
        optional = true,
        description = "Priority for Cloud Spanner RPC invocations",
        helpText =
            "The request priority for Spanner calls. Possible values are `HIGH`, `MEDIUM`, and"
                + " `LOW`. The default value is `MEDIUM`.")
    RpcPriority getSpannerPriority();

    void setSpannerPriority(RpcPriority value);

    @TemplateParameter.Integer(
        groupName = "Target",
        order = 7,
        optional = true,
        description = "Cloud Spanner batch size",
        helpText = "How many rows to process on each batch. The default value is 100.",
        example = "100")
    @Default.Integer(100)
    Integer getSpannerBatchSize();

    void setSpannerBatchSize(Integer value);

    @TemplateParameter.Text(
        groupName = "Schema",
        order = 8,
        description = "Cloud Spanner table name",
        helpText = "Name of the Spanner table where to upsert data.")
    String getTableName();

    void setTableName(String value);

    @TemplateParameter.Enum(
        groupName = "Schema",
        order = 9,
        optional = true,
        enumOptions = {@TemplateEnumOption("TYPE_1"), @TemplateEnumOption("TYPE_2")},
        description = "Slow Changing Dimension (SCD) type",
        helpText =
            "Type of SCD which will be applied when writing to Spanner. The default value is"
                + " TYPE_1.",
        example = "TYPE_1 or TYPE_2")
    @Default.Enum("TYPE_1")
    AvroToSpannerScdOptions.ScdType getScdType();

    void setScdType(AvroToSpannerScdOptions.ScdType value);

    @TemplateParameter.Text(
        groupName = "Schema",
        order = 10,
        description = "Primary key column name(s)",
        helpText =
            "Name of column(s) for the primary key(s). If more than one, enter as CSV with no"
                + " spaces (e.g. column1,column2).")
    List<String> getPrimaryKeyColumnNames();

    void setPrimaryKeyColumnNames(List<String> value);

    @TemplateParameter.Text(
        groupName = "Schema",
        order = 11,
        optional = true,
        description = "Order by column name",
        helpText =
            "Name of column that will be used to order when there are multiple updates for "
                + "the same primary key within the same file.")
    String getOrderByColumnName();

    void setOrderByColumnName(String value);

    @TemplateParameter.Enum(
        groupName = "Schema",
        order = 12,
        optional = true,
        enumOptions = {@TemplateEnumOption("ASC"), @TemplateEnumOption("DESC")},
        description = "Order in Ascending (ASC) or Descending (DESC) order",
        helpText = "Whether to use ASC or DESC when sorting the records by order by column name.",
        example = "ASC")
    @Default.Enum("ASC")
    OrderByOrder getOrderByOrder();

    void setOrderByOrder(OrderByOrder value);

    @TemplateParameter.Text(
        groupName = "Schema",
        order = 13,
        optional = true,
        description = "Start date column name",
        helpText = "Name of column name for the start date (TIMESTAMP). Only used for SCD-Type=2.")
    String getStartDateColumnName();

    void setStartDateColumnName(String value);

    @TemplateParameter.Text(
        groupName = "Schema",
        order = 14,
        optional = true,
        description = "End date column name",
        helpText =
            "Name of column name for the end date (TIMESTAMP). Only required for" + " SCD-Type=2.")
    String getEndDateColumnName();

    void setEndDateColumnName(String value);

    @TemplateCreationParameter(value = "false")
    @Description("If true, wait for job finish.")
    @Default.Boolean(true)
    boolean getWaitUntilFinish();

    void setWaitUntilFinish(boolean value);

    enum ScdType {
      TYPE_1,
      TYPE_2,
    }

    enum OrderByOrder {
      ASC,
      DESC,
    }
  }
}
