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

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.OptionsToConfigBuilder;
import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.reader.ReaderImpl;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.JdbcIoWrapper;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.reader.io.transform.ReaderTransform;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.transformer.SourceRowToMutationDoFn;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.Write;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

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
    Pipeline pipeline = Pipeline.create(options);
    options.as(SdkHarnessOptions.class).setDefaultSdkHarnessLogLevel(options.getDefaultLogLevel());

    ReaderImpl reader =
        ReaderImpl.of(
            JdbcIoWrapper.of(
                OptionsToConfigBuilder.MySql.configWithMySqlDefaultsFromOptions(options)));
    SourceSchema srcSchema = reader.getSourceSchema();
    ReaderTransform readerTransform = reader.getReaderTransform();

    PCollectionTuple rowsAndTables = pipeline.apply("Read rows", readerTransform.readTransform());
    PCollection<SourceRow> sourceRows = rowsAndTables.get(readerTransform.sourceRowTag());

    SourceRowToMutationDoFn transformDoFn =
        SourceRowToMutationDoFn.create(getSchemaMapper(options), getTableIDToRefMap(srcSchema));
    PCollection<Mutation> mutations = sourceRows.apply("Transform", ParDo.of(transformDoFn));
    mutations.apply("Write", getSpannerWrite(options));

    return pipeline.run();
  }

  private static ISchemaMapper getSchemaMapper(SourceDbToSpannerOptions options) {
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(ValueProvider.StaticValueProvider.of(options.getProjectId()))
            .withHost(ValueProvider.StaticValueProvider.of(options.getSpannerHost()))
            .withInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()))
            .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getDatabaseId()));
    Ddl ddl = getInformationSchemaAsDdl(spannerConfig);
    ISchemaMapper schemaMapper = new IdentityMapper(ddl);
    if (options.getSessionFilePath() != null && !options.getSessionFilePath().equals("")) {
      schemaMapper = new SessionBasedMapper(options.getSessionFilePath(), ddl);
    }
    return schemaMapper;
  }

  // TODO: SpannerInfoschema scanner code is duplicated across live, bulk and reverse replication
  // templates. We should refactor everything to spanner-common.
  private static Ddl getInformationSchemaAsDdl(SpannerConfig spannerConfig) {
    SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    DatabaseAdminClient databaseAdminClient = spannerAccessor.getDatabaseAdminClient();
    Dialect dialect =
        databaseAdminClient
            .getDatabase(spannerConfig.getInstanceId().get(), spannerConfig.getDatabaseId().get())
            .getDialect();
    BatchClient batchClient = spannerAccessor.getBatchClient();
    BatchReadOnlyTransaction context =
        batchClient.batchReadOnlyTransaction(TimestampBound.strong());
    InformationSchemaScanner scanner = new InformationSchemaScanner(context, dialect);
    Ddl ddl = scanner.scan();
    spannerAccessor.close();
    return ddl;
  }

  private static Map<String, SourceTableReference> getTableIDToRefMap(SourceSchema srcSchema) {
    Map<String, SourceTableReference> tableIdMapper = new HashMap<>();
    for (SourceTableSchema srcTableSchema : srcSchema.tableSchemas()) {
      tableIdMapper.put(
          srcTableSchema.tableSchemaUUID(),
          SourceTableReference.builder()
              .setSourceSchemaReference(srcSchema.schemaReference())
              .setSourceTableName(srcTableSchema.tableName())
              .setSourceTableSchemaUUID(srcTableSchema.tableSchemaUUID())
              .build());
    }
    return tableIdMapper;
  }

  private static Write getSpannerWrite(SourceDbToSpannerOptions options) {
    return SpannerIO.write()
        .withProjectId(options.getProjectId())
        .withInstanceId(options.getInstanceId())
        .withDatabaseId(options.getDatabaseId());
  }
}
