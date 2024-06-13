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

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.constants.SourceDbToSpannerConstants;
import com.google.cloud.teleport.v2.options.OptionsToConfigBuilder;
import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.reader.ReaderImpl;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.JdbcIoWrapper;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.source.reader.io.transform.ReaderTransform;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidOptionsException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.spanner.SpannerSchema;
import com.google.cloud.teleport.v2.transformer.SourceRowToMutationDoFn;
import com.google.cloud.teleport.v2.writer.DeadLetterQueue;
import com.google.cloud.teleport.v2.writer.SpannerWriter;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A template that copies data from a relational database using JDBC to an existing Spanner
 * database.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/sourcedb-to-spanner/README_Sourcedb_to_Spanner_Flex.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Sourcedb_to_Spanner_Flex",
    category = TemplateCategory.BATCH,
    displayName = "Sourcedb to Spanner",
    description = {
      "The SourceDB to Spanner template is a batch pipeline that copies data from a relational"
          + " database into an existing Spanner database. This pipeline uses JDBC to connect to"
          + " the relational database. You can use this template to copy data from any relational"
          + " database with available JDBC drivers into Spanner. This currently only supports a limited set of types of MySQL",
      "For an extra layer of protection, you can also pass in a Cloud KMS key along with a"
          + " Base64-encoded username, password, and connection string parameters encrypted with"
          + " the Cloud KMS key. See the <a"
          + " href=\"https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt\">Cloud"
          + " KMS API encryption endpoint</a> for additional details on encrypting your username,"
          + " password, and connection string parameters."
    },
    optionsClass = SourceDbToSpannerOptions.class,
    flexContainerName = "source-db-to-spanner",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/sourcedb-to-spanner",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "The JDBC drivers for the relational database must be available.",
      "The Spanner tables must exist before pipeline execution.",
      "The Spanner tables must have a compatible schema.",
      "The relational database must be accessible from the subnet where Dataflow runs."
    })
public class SourceDbToSpanner {

  private static final Logger LOG = LoggerFactory.getLogger(SourceDbToSpanner.class);

  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link SourceDbToSpanner#run} method to start the
   * pipeline and invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    // Parse the user options passed from the command-line
    SourceDbToSpannerOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SourceDbToSpannerOptions.class);
    run(options);
  }

  /**
   * Create the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  @VisibleForTesting
  static PipelineResult run(SourceDbToSpannerOptions options) {
    // TODO - Validate if options are as expected
    Pipeline pipeline = Pipeline.create(options);

    SpannerConfig spannerConfig = createSpannerConfig(options);
    Ddl ddl = SpannerSchema.getInformationSchemaAsDdl(spannerConfig);
    ISchemaMapper schemaMapper = getSchemaMapper(options, ddl);

    List<String> tablesToMigrate = listTablesToMigrate(options, schemaMapper, ddl);

    // Read data from source
    ReaderImpl reader =
        ReaderImpl.of(
            JdbcIoWrapper.of(
                OptionsToConfigBuilder.MySql.configWithMySqlDefaultsFromOptions(
                    options, tablesToMigrate)));
    SourceSchema srcSchema = reader.getSourceSchema();

    ReaderTransform readerTransform = reader.getReaderTransform();

    PCollectionTuple rowsAndTables = pipeline.apply("Read rows", readerTransform.readTransform());
    PCollection<SourceRow> sourceRows = rowsAndTables.get(readerTransform.sourceRowTag());

    // Transform source data to Spanner Compatible Data
    SourceRowToMutationDoFn transformDoFn = SourceRowToMutationDoFn.create(schemaMapper);
    PCollectionTuple transformationResult =
        sourceRows.apply(
            "Transform",
            ParDo.of(transformDoFn)
                .withOutputTags(
                    SourceDbToSpannerConstants.ROW_TRANSFORMATION_SUCCESS,
                    TupleTagList.of(SourceDbToSpannerConstants.ROW_TRANSFORMATION_ERROR)));

    // Write to Spanner
    SpannerWriter writer = new SpannerWriter(spannerConfig);
    PCollection<MutationGroup> failedMutations =
        writer.writeToSpanner(
            transformationResult
                .get(SourceDbToSpannerConstants.ROW_TRANSFORMATION_SUCCESS)
                .setCoder(SerializableCoder.of(RowContext.class)));

    // Dump Failed rows to DLQ
    DeadLetterQueue dlq = DeadLetterQueue.create(options.getDLQDirectory(), ddl);
    dlq.failedMutationsToDLQ(failedMutations);
    dlq.failedTransformsToDLQ(
        transformationResult
            .get(SourceDbToSpannerConstants.ROW_TRANSFORMATION_ERROR)
            .setCoder(SerializableCoder.of(RowContext.class)));
    return pipeline.run();
  }

  @VisibleForTesting
  static SpannerConfig createSpannerConfig(SourceDbToSpannerOptions options) {
    return SpannerConfig.create()
        .withProjectId(ValueProvider.StaticValueProvider.of(options.getProjectId()))
        .withHost(ValueProvider.StaticValueProvider.of(options.getSpannerHost()))
        .withInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()))
        .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getDatabaseId()));
  }

  @VisibleForTesting
  static ISchemaMapper getSchemaMapper(SourceDbToSpannerOptions options, Ddl ddl) {
    ISchemaMapper schemaMapper = new IdentityMapper(ddl);
    if (options.getSessionFilePath() != null && !options.getSessionFilePath().equals("")) {
      schemaMapper = new SessionBasedMapper(options.getSessionFilePath(), ddl);
    }
    return schemaMapper;
  }

  /*
   * Return the available tables to migrate based on the following.
   * 1. Fetch tables from schema mapper. Override with tables from options if present
   * 2. Mark for migration if tables have corresponding spanner tables.
   * Err on the side of being lenient with configuration
   */
  static List<String> listTablesToMigrate(
      SourceDbToSpannerOptions options, ISchemaMapper mapper, Ddl ddl) {
    List<String> tablesFromOptions =
        StringUtils.isNotBlank(options.getTables())
            ? Arrays.stream(options.getTables().split(",")).collect(Collectors.toList())
            : new ArrayList<String>();

    List<String> sourceTablesConfigured = null;
    if (tablesFromOptions.isEmpty()) {
      sourceTablesConfigured = mapper.getSourceTablesToMigrate("");
      LOG.info("using tables from mapper as no overrides provided: {}", sourceTablesConfigured);
    } else {
      LOG.info("table overrides configured: {}", tablesFromOptions);
      sourceTablesConfigured = tablesFromOptions;
    }

    List<String> tablesToMigrate = new ArrayList<>();
    for (String srcTable : sourceTablesConfigured) {
      String spannerTable = null;
      try {
        spannerTable = mapper.getSpannerTableName("", srcTable);
      } catch (NoSuchElementException e) {
        LOG.info("could not fetch spanner table from mapper: {}", srcTable);
        continue;
      }

      if (spannerTable == null) {
        LOG.warn("skipping source table as there is no mapped spanner table: {} ", spannerTable);
      } else if (ddl.table(spannerTable) == null) {
        LOG.warn(
            "skipping source table: {} as there is no matching spanner table: {} ",
            srcTable,
            spannerTable);
      } else {
        // source table has matching spanner table on current spanner instance
        tablesToMigrate.add(srcTable);
      }
    }

    if (tablesToMigrate.isEmpty()) {
      LOG.error("aborting migration as no tables found to migrate");
      throw new InvalidOptionsException("no configured tables can be migrated");
    }
    return tablesToMigrate;
  }
}
