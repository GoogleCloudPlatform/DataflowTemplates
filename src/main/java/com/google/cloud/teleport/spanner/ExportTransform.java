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
package com.google.cloud.teleport.spanner;

import static com.google.cloud.teleport.spanner.SpannerTableFilter.getFilteredTables;
import static com.google.cloud.teleport.util.ValueProviderUtils.eitherOrValueProvider;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.spanner.ddl.ChangeStream;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.Table;
import com.google.cloud.teleport.spanner.proto.ExportProtos;
import com.google.cloud.teleport.spanner.proto.ExportProtos.Export;
import com.google.cloud.teleport.spanner.proto.ExportProtos.ProtoDialect;
import com.google.cloud.teleport.spanner.proto.ExportProtos.TableManifest;
import com.google.cloud.teleport.templates.common.SpannerConverters.CreateTransactionFnWithTimestamp;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.DynamicAvroDestinations;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.spanner.LocalSpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Requirements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline exports the complete contents of a Cloud Spanner database to GCS. Each table is
 * exported as a set of Avro files. In addition to Avro files the pipeline also exports a
 * manifest.json file per table, and an export summary file 'spanner-export.json'.
 *
 * <p>For example, for a database with two tables Users and Singers, the pipeline will export the
 * following file set. <code>
 *  Singers-manifest.json
 *  Singers.avro-00000-of-00002
 *  Singers.avro-00001-of-00002
 *  Users-manifest.json
 *  Users.avro-00000-of-00003
 *  Users.avro-00001-of-00003
 *  Users.avro-00002-of-00003
 *  spanner-export.json
 * </code>
 */
public class ExportTransform extends PTransform<PBegin, WriteFilesResult<String>> {
  private static final Logger LOG = LoggerFactory.getLogger(ExportTransform.class);

  private static final String EMPTY_EXPORT_FILE = "empty-cloud-spanner-export";

  private final SpannerConfig spannerConfig;
  private final ValueProvider<String> outputDir;
  private final ValueProvider<String> testJobId;
  private final ValueProvider<String> snapshotTime;
  private final ValueProvider<String> tableNames;
  private final ValueProvider<Boolean> exportRelatedTables;
  private final ValueProvider<Boolean> shouldExportTimestampAsLogicalType;
  private final ValueProvider<String> avroTempDirectory;

  public ExportTransform(
      SpannerConfig spannerConfig,
      ValueProvider<String> outputDir,
      ValueProvider<String> testJobId) {
    this(
        spannerConfig,
        outputDir,
        testJobId,
        /*snapshotTime=*/ ValueProvider.StaticValueProvider.of(""),
        /*tableNames=*/ ValueProvider.StaticValueProvider.of(""),
        /*exportRelatedTables=*/ ValueProvider.StaticValueProvider.of(false),
        /*shouldExportTimestampAsLogicalType=*/ ValueProvider.StaticValueProvider.of(false),
        outputDir);
  }

  public ExportTransform(
      SpannerConfig spannerConfig,
      ValueProvider<String> outputDir,
      ValueProvider<String> testJobId,
      ValueProvider<String> snapshotTime,
      ValueProvider<String> tableNames,
      ValueProvider<Boolean> exportRelatedTables,
      ValueProvider<Boolean> shouldExportTimestampAsLogicalType,
      ValueProvider<String> avroTempDirectory) {
    this.spannerConfig = spannerConfig;
    this.outputDir = outputDir;
    this.testJobId = testJobId;
    this.snapshotTime = snapshotTime;
    this.tableNames = tableNames;
    this.exportRelatedTables = exportRelatedTables;
    this.shouldExportTimestampAsLogicalType = shouldExportTimestampAsLogicalType;
    this.avroTempDirectory = avroTempDirectory;
  }

  /**
   * Read the Cloud Spanner schema and all the rows in all the tables of the database. Create and
   * write the exported Avro files to GCS.
   */
  @Override
  public WriteFilesResult<String> expand(PBegin begin) {
    Pipeline p = begin.getPipeline();

    /*
     * Allow users to specify read timestamp.
     * CreateTransaction and CreateTransactionFn classes in LocalSpannerIO
     * only take a timestamp object for exact staleness which works when
     * parameters are provided during template compile time. They do not work with
     * a Timestamp valueProvider which can take parameters at runtime. Hence a new
     * ParDo class CreateTransactionFnWithTimestamp had to be created for this
     * purpose.
     */
    PCollectionView<Transaction> tx =
        p.apply("CreateTransaction", Create.of(1))
            .apply(
                "Create transaction",
                ParDo.of(new CreateTransactionFnWithTimestamp(spannerConfig, snapshotTime)))
            .apply("Tx As PCollectionView", View.asSingleton());

    PCollectionView<Dialect> dialectView =
        p.apply("Read Dialect", new ReadDialect(spannerConfig))
            .apply("Dialect As PCollectionView", View.asSingleton());

    PCollection<Ddl> ddl =
        p.apply(
            "Read Information Schema", new ReadInformationSchema(spannerConfig, tx, dialectView));

    PCollection<Ddl> exportState =
        ddl.apply(
            "Check export conditions",
            ParDo.of(
                new DoFn<Ddl, Ddl>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) throws Exception {
                    Ddl ddl = c.element();
                    List<String> tablesList = Collections.emptyList();

                    // If the user sets shouldRelatedTables to true without providing
                    // a list of export tables, throw an exception.
                    if (tableNames.get().trim().isEmpty() && exportRelatedTables.get()) {
                      throw new Exception(
                          "Invalid usage of --tableNames and --shouldExportRelatedTables. Set"
                              + " --shouldExportRelatedTables=true only if --tableNames is given"
                              + " selected tables for export.");
                    }

                    // If the user provides a comma-separated list of strings, parse it into a List
                    if (!tableNames.get().trim().isEmpty()) {
                      tablesList = Arrays.asList(tableNames.get().split(",\\s*"));
                    }

                    // If the user provided any invalid table names, throw an exception.
                    List<String> allSpannerTables =
                        ddl.allTables().stream().map(t -> t.name()).collect(Collectors.toList());

                    List<String> invalidTables =
                        tablesList.stream()
                            .distinct()
                            .filter(t -> !allSpannerTables.contains(t))
                            .collect(Collectors.toList());

                    if (invalidTables.size() != 0) {
                      throw new Exception(
                          "INVALID_ARGUMENT: Table(s) not found: "
                              + String.join(", ", invalidTables)
                              + ".");
                    }

                    List<String> filteredTables =
                        getFilteredTables(ddl, tablesList).stream()
                            .map(t -> t.name())
                            .collect(Collectors.toList());

                    // Save any missing necessary export table names; save a copy of the original
                    // table list to bypass 'final or effectively final' condition of the lambda
                    // expression below.
                    List<String> usersTables = tablesList.stream().collect(Collectors.toList());
                    List<String> missingTables =
                        filteredTables.stream()
                            .distinct()
                            .filter(t -> !usersTables.contains(t))
                            .collect(Collectors.toList());
                    Collections.sort(missingTables);

                    // If user has specified a list of tables without including required
                    // related tables, and not explicitly set shouldExportRelatedTables,
                    // throw an exception.
                    if (tablesList.size() != 0
                        && !(tablesList.equals(filteredTables))
                        && !exportRelatedTables.get()) {
                      throw new Exception(
                          "Attempted to export table(s) requiring parent and/or foreign keys tables"
                              + " without setting the shouldExportRelatedTables parameter. Set"
                              + " --shouldExportRelatedTables=true to export all necessary"
                              + " tables, or add "
                              + String.join(", ", missingTables)
                              + " to --tableNames.");
                    }
                    c.output(ddl);
                  }
                }));
    PCollection<ReadOperation> tables =
        ddl.apply("Build table read operations", new BuildReadFromTableOperations(tableNames));

    PCollection<KV<String, Void>> allTableAndViewNames =
        ddl.apply(
            "List all table and view names",
            ParDo.of(
                new DoFn<Ddl, KV<String, Void>>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Ddl ddl = c.element();
                    for (Table t : ddl.allTables()) {
                      c.output(KV.of(t.name(), null));
                    }
                    // We want the resulting collection to contain the names of all entities that
                    // need to be exported, both tables and views.  Ddl holds these separately, so
                    // we need to add the names of all views separately here.
                    for (com.google.cloud.teleport.spanner.ddl.View v : ddl.views()) {
                      c.output(KV.of(v.name(), null));
                    }
                  }
                }));

    PCollection<String> allChangeStreamNames =
        ddl.apply(
            "List all change stream names",
            ParDo.of(
                new DoFn<Ddl, String>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Ddl ddl = c.element();
                    for (ChangeStream changeStream : ddl.changeStreams()) {
                      c.output(changeStream.name());
                    }
                  }
                }));

    // Generate a unique output directory name.
    final PCollectionView<String> outputDirectoryName =
        p.apply(Create.of(1))
            .apply(
                "Create Avro output folder",
                ParDo.of(
                    new DoFn<Integer, String>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        String instanceId = spannerConfig.getInstanceId().get();
                        String dbId = spannerConfig.getDatabaseId().get();
                        // For direct runner or tests we need a deterministic jobId.
                        String testJobId = ExportTransform.this.testJobId.get();
                        if (!Strings.isNullOrEmpty(testJobId)) {
                          c.output(testJobId);
                          return;
                        }
                        try {
                          DataflowWorkerHarnessOptions workerHarnessOptions =
                              c.getPipelineOptions().as(DataflowWorkerHarnessOptions.class);
                          String jobId = workerHarnessOptions.getJobId();
                          c.output(instanceId + "-" + dbId + "-" + jobId);
                        } catch (Exception e) {
                          throw new IllegalStateException(
                              "Please specify --testJobId to run with non-dataflow runner");
                        }
                      }
                    }))
            .apply(View.asSingleton());

    final PCollectionView<Map<String, SerializableSchemaSupplier>> avroSchemas =
        ddl.apply(
                "Build Avro schemas from DDL",
                ParDo.of(
                    new DoFn<Ddl, KV<String, SerializableSchemaSupplier>>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        Collection<Schema> avroSchemas =
                            new DdlToAvroSchemaConverter(
                                    "spannerexport",
                                    "1.0.0",
                                    shouldExportTimestampAsLogicalType.get())
                                .convert(c.element());
                        for (Schema schema : avroSchemas) {
                          c.output(KV.of(schema.getName(), new SerializableSchemaSupplier(schema)));
                        }
                      }
                    }))
            .apply("As view", View.asMap());

    PCollection<Struct> rows =
        tables.apply(
            "Read all rows from Spanner",
            LocalSpannerIO.readAll().withTransaction(tx).withSpannerConfig(spannerConfig));

    ValueProvider<ResourceId> resource =
        ValueProvider.NestedValueProvider.of(
            outputDir,
            (SerializableFunction<String, ResourceId>) s -> FileSystems.matchNewResource(s, true));

    ValueProvider<ResourceId> tempResource =
        ValueProvider.NestedValueProvider.of(
            eitherOrValueProvider(avroTempDirectory, outputDir),
            (SerializableFunction<String, ResourceId>) s -> FileSystems.matchNewResource(s, true));

    WriteFilesResult<String> fileWriteResults =
        rows.apply(
            "Store Avro files",
            AvroIO.<Struct>writeCustomTypeToGenericRecords()
                .to(
                    new SchemaBasedDynamicDestinations(
                        avroSchemas, outputDirectoryName, dialectView, resource))
                .withTempDirectory(tempResource));

    // Generate the manifest file.
    PCollection<KV<String, Iterable<String>>> tableFiles =
        fileWriteResults.getPerDestinationOutputFilenames().apply(GroupByKey.create());

    final TupleTag<Void> allTables = new TupleTag<>();
    final TupleTag<Iterable<String>> nonEmptyTables = new TupleTag<>();

    PCollection<KV<String, CoGbkResult>> groupedTables =
        KeyedPCollectionTuple.of(allTables, allTableAndViewNames)
            .and(nonEmptyTables, tableFiles)
            .apply("Group with all tables", CoGroupByKey.create());

    // The following is to export empty tables and views from the database.  Empty tables and views
    // are handled together because we do not export any rows for views, only their metadata,
    // including the queries defining them.
    PCollection<KV<String, Iterable<String>>> emptyTablesAndViews =
        groupedTables.apply(
            "Export empty tables and views",
            ParDo.of(
                new DoFn<KV<String, CoGbkResult>, KV<String, Iterable<String>>>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    KV<String, CoGbkResult> kv = c.element();
                    String table = kv.getKey();
                    CoGbkResult coGbkResult = kv.getValue();
                    Iterable<String> only = coGbkResult.getOnly(nonEmptyTables, null);
                    if (only == null) {
                      LOG.info("Exporting empty table or view: " + table);
                      // This file will contain the schema definition: column definitions for empty
                      // tables or defining queries for views.
                      c.output(KV.of(table, Collections.singleton(table + ".avro-00000-of-00001")));
                    }
                  }
                }));

    PCollection<KV<String, Iterable<String>>> changeStreams =
        allChangeStreamNames.apply(
            "Export change streams",
            ParDo.of(
                new DoFn<String, KV<String, Iterable<String>>>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    String changeStreamName = c.element();
                    LOG.info("Exporting change stream: " + changeStreamName);
                    // This file will contain the schema definition for the change stream.
                    c.output(
                        KV.of(
                            changeStreamName,
                            Collections.singleton(changeStreamName + ".avro-00000-of-00001")));
                  }
                }));

    // Empty tables, views and change streams are handled together, because we export them as empty
    // Avro files that only contain the Avro schemas.
    PCollection<KV<String, Iterable<String>>> emptySchemaFiles =
        PCollectionList.of(emptyTablesAndViews)
            .and(changeStreams)
            .apply("Combine all empty schema files", Flatten.pCollections());

    emptySchemaFiles =
        emptySchemaFiles.apply(
            "Save empty schema files",
            ParDo.of(
                    new DoFn<KV<String, Iterable<String>>, KV<String, Iterable<String>>>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        Map<String, SerializableSchemaSupplier> schemaMap =
                            c.sideInput(avroSchemas);
                        KV<String, Iterable<String>> kv = c.element();
                        String objectName = kv.getKey();
                        String fileName = kv.getValue().iterator().next();

                        Schema schema = schemaMap.get(objectName).get();

                        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

                        Path fullPath =
                            createOutputPath(
                                outputDir.get(), c.sideInput(outputDirectoryName), fileName);

                        try (DataFileWriter<GenericRecord> dataFileWriter =
                            new DataFileWriter<>(datumWriter)) {
                          dataFileWriter.create(schema, createOutputStream(fullPath, c));
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                        c.output(KV.of(objectName, Collections.singleton(fullPath.toString())));
                      }

                      /**
                       * Resolves the complete path name for Avro files for both GCS and local FS
                       * (for testing).
                       *
                       * @param outputDirectoryPath Initial directory path for the file.
                       * @param outputDirectoryName Terminal directory for the file.
                       * @param fileName Name of the Avro file
                       * @return The full {@link Path} of the output Avro file.
                       */
                      private Path createOutputPath(
                          String outputDirectoryPath, String outputDirectoryName, String fileName) {
                        if (GcsPath.GCS_URI.matcher(outputDirectoryPath).matches()) {
                          // Avro file path in GCS.
                          return GcsPath.fromUri(outputDirectoryPath)
                              .resolve(outputDirectoryName)
                              .resolve(fileName);
                        } else {
                          // Avro file path in local filesystem
                          return Paths.get(outputDirectoryPath, outputDirectoryName, fileName);
                        }
                      }

                      /**
                       * Creates the {@link OutputStream} for the output file either on GCS or on
                       * local FS (for testing).
                       *
                       * @param outputPath The full path of the output file.
                       * @param c The {@link org.apache.beam.sdk.transforms.DoFn.ProcessContext}
                       * @return An {@link OutputStream} for the opened output file.
                       * @throws IOException if the output file cannot be opened.
                       */
                      private OutputStream createOutputStream(Path outputPath, ProcessContext c)
                          throws IOException {
                        if (GcsPath.GCS_URI.matcher(outputPath.toString()).matches()) {
                          // Writing the Avro file to GCS.
                          org.apache.beam.sdk.extensions.gcp.util.GcsUtil gcsUtil =
                              c.getPipelineOptions().as(GcsOptions.class).getGcsUtil();
                          String gcsType = "application/octet-stream";
                          WritableByteChannel gcsChannel =
                              gcsUtil.create((GcsPath) outputPath, gcsType);
                          return Channels.newOutputStream(gcsChannel);
                        } else {
                          // Avro file is created on local filesystem (for testing).
                          Files.createDirectories(outputPath.getParent());
                          return Files.newOutputStream(outputPath);
                        }
                      }
                    })
                .withSideInputs(avroSchemas, outputDirectoryName));

    PCollection<KV<String, Iterable<String>>> allFiles =
        PCollectionList.of(tableFiles)
            .and(emptySchemaFiles)
            .apply("Combine all files", Flatten.pCollections());

    PCollection<KV<String, String>> tableManifests =
        allFiles.apply("Build table manifests", ParDo.of(new BuildTableManifests()));

    Contextful.Fn<String, FileIO.Write.FileNaming> tableManifestNaming =
        (element, c) ->
            (window, pane, numShards, shardIndex, compression) ->
                GcsUtil.joinPath(
                    outputDir.get(),
                    c.sideInput(outputDirectoryName),
                    tableManifestFileName(element));

    tableManifests.apply(
        "Store table manifests",
        FileIO.<String, KV<String, String>>writeDynamic()
            .by(KV::getKey)
            .withDestinationCoder(StringUtf8Coder.of())
            .withNaming(
                Contextful.of(
                    tableManifestNaming, Requirements.requiresSideInputs(outputDirectoryName)))
            .via(Contextful.fn(KV::getValue), TextIO.sink())
            .withTempDirectory(eitherOrValueProvider(avroTempDirectory, outputDir)));

    PCollection<List<Export.Table>> metadataTables =
        tableManifests.apply(
            "Combine table metadata", Combine.globally(new CombineTableMetadata()));

    PCollectionView<Ddl> ddlView = ddl.apply("Cloud Spanner DDL as view", View.asSingleton());

    PCollection<String> metadataContent =
        metadataTables.apply(
            "Create database manifest",
            ParDo.of(new CreateDatabaseManifest(ddlView, dialectView))
                .withSideInputs(ddlView, dialectView));

    Contextful.Fn<String, FileIO.Write.FileNaming> manifestNaming =
        (element, c) ->
            (window, pane, numShards, shardIndex, compression) ->
                GcsUtil.joinPath(
                    outputDir.get(), c.sideInput(outputDirectoryName), "spanner-export.json");

    metadataContent.apply(
        "Store the database manifest",
        FileIO.<String, String>writeDynamic()
            .by(SerializableFunctions.constant(""))
            .withDestinationCoder(StringUtf8Coder.of())
            .via(TextIO.sink())
            .withNaming(
                Contextful.of(manifestNaming, Requirements.requiresSideInputs(outputDirectoryName)))
            .withTempDirectory(eitherOrValueProvider(avroTempDirectory, outputDir)));
    return fileWriteResults;
  }

  /** Saves {@link Struct} elements (rows from Spanner) to destination Avro files. */
  @VisibleForTesting
  static class SchemaBasedDynamicDestinations
      extends DynamicAvroDestinations<Struct, String, GenericRecord> {

    private final PCollectionView<Map<String, SerializableSchemaSupplier>> avroSchemas;
    private final PCollectionView<String> uniqueIdView;
    private final PCollectionView<Dialect> dialectView;
    private final ValueProvider<ResourceId> baseDir;

    SchemaBasedDynamicDestinations(
        PCollectionView<Map<String, SerializableSchemaSupplier>> avroSchemas,
        PCollectionView<String> uniqueIdView,
        PCollectionView<Dialect> dialectView,
        ValueProvider<ResourceId> baseDir) {
      this.avroSchemas = avroSchemas;
      this.uniqueIdView = uniqueIdView;
      this.dialectView = dialectView;
      this.baseDir = baseDir;
    }

    @Override
    public Schema getSchema(String tableName) {
      Map<String, SerializableSchemaSupplier> si = sideInput(avroSchemas);
      // Check if there are any schemas available or if the table it is EMPTY_EXPORT_FILE
      if (si.isEmpty() || tableName.equals(EMPTY_EXPORT_FILE)) {
        // The EMPTY_EXPORT_FILE still needs to have a rudimentary schema for it to be created.
        return SchemaBuilder.record("Empty").fields().endRecord();
      }
      return si.get(tableName).get();
    }

    @Override
    public String getDestination(Struct element) {
      // The input is a PCollection of rows from all tables.
      // That has to be demultiplexed into a separate destination for each table.
      // This is done using the first element of each table that is the table name.
      return element.getString(0);
    }

    @Override
    public String getDefaultDestination() {
      // Create a default file if there is absolutely no tables in the exported database.
      return EMPTY_EXPORT_FILE;
    }

    @Override
    public FileBasedSink.FilenamePolicy getFilenamePolicy(final String destination) {
      final String uniqueId = sideInput(uniqueIdView);
      return DefaultFilenamePolicy.fromStandardParameters(
          ValueProvider.NestedValueProvider.of(
              baseDir,
              (SerializableFunction<ResourceId, ResourceId>)
                  r ->
                      r.resolve(
                          GcsUtil.joinPath(uniqueId, destination + ".avro"),
                          ResolveOptions.StandardResolveOptions.RESOLVE_FILE)),
          null,
          null,
          false);
    }

    @Override
    public List<PCollectionView<?>> getSideInputs() {
      return Arrays.asList(avroSchemas, uniqueIdView, dialectView);
    }

    @Override
    public GenericRecord formatRecord(Struct record) {
      String table = record.getString(0);
      Schema schema = sideInput(avroSchemas).get(table).get();
      Dialect dialect = sideInput(dialectView);
      return new SpannerRecordConverter(schema, dialect).convert(record);
    }
  }

  // TODO: use AvroUtils.serializableSchemaSupplier once it is public
  @VisibleForTesting
  static class SerializableSchemaString implements Serializable {

    private final String schema;

    SerializableSchemaString(String schema) {
      this.schema = schema;
    }

    Object readResolve() throws IOException, ClassNotFoundException {
      return new SerializableSchemaSupplier(Schema.parse(schema));
    }
  }

  @VisibleForTesting
  static class SerializableSchemaSupplier implements Serializable, Supplier<Schema> {

    private final Schema schema;

    SerializableSchemaSupplier(Schema schema) {
      this.schema = schema;
    }

    Object writeReplace() {
      return new SerializableSchemaString(schema.toString());
    }

    @Override
    public Schema get() {
      return schema;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      SerializableSchemaSupplier that = (SerializableSchemaSupplier) o;

      return schema != null ? schema.equals(that.schema) : that.schema == null;
    }

    @Override
    public int hashCode() {
      return schema != null ? schema.hashCode() : 0;
    }
  }

  /** Given map of table names, create the database manifest contents. */
  static class CombineTableMetadata
      extends CombineFn<KV<String, String>, List<Export.Table>, List<Export.Table>> {

    @Override
    public List<Export.Table> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<Export.Table> addInput(List<Export.Table> accumulator, KV<String, String> input) {
      ExportProtos.Export.Table.Builder tablesBuilder = ExportProtos.Export.Table.newBuilder();

      String tableName = input.getKey();
      tablesBuilder.setName(tableName);
      tablesBuilder.setManifestFile(tableManifestFileName(tableName));

      accumulator.add(tablesBuilder.build());
      return accumulator;
    }

    @Override
    public List<Export.Table> mergeAccumulators(Iterable<List<Export.Table>> accumulators) {
      List<Export.Table> result = new ArrayList<>();
      for (List<Export.Table> acc : accumulators) {
        result.addAll(acc);
      }
      return result;
    }

    @Override
    public List<Export.Table> extractOutput(List<Export.Table> accumulator) {
      return accumulator;
    }
  }

  @VisibleForTesting
  static class CreateDatabaseManifest extends DoFn<List<Export.Table>, String> {

    private final PCollectionView<Ddl> ddlView;
    private final PCollectionView<Dialect> dialectView;

    public CreateDatabaseManifest(
        PCollectionView<Ddl> ddlView, PCollectionView<Dialect> dialectView) {
      this.ddlView = ddlView;
      this.dialectView = dialectView;
    }

    @ProcessElement
    public void processElement(
        @Element List<Export.Table> exportMetadata, OutputReceiver<String> out, ProcessContext c) {
      Ddl ddl = c.sideInput(ddlView);
      Dialect dialect = c.sideInput(dialectView);
      ExportProtos.Export.Builder exportManifest = ExportProtos.Export.newBuilder();
      for (Export.Table obj : exportMetadata) {
        if (ddl.changeStream(obj.getName()) != null) {
          exportManifest.addChangeStreams(obj);
        } else {
          exportManifest.addTables(obj);
        }
      }
      exportManifest.addAllDatabaseOptions(ddl.databaseOptions());
      exportManifest.setDialect(ProtoDialect.valueOf(dialect.name()));
      try {
        out.output(JsonFormat.printer().print(exportManifest.build()));
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static String tableManifestFileName(String tableName) {
    return tableName + "-manifest.json";
  }

  @VisibleForTesting
  static TimestampBound createTimestampBound(String timestamp) {
    if ("".equals(timestamp)) {
      /* If no timestamp is specified, read latest data */
      return TimestampBound.strong();
    } else {
      /* Else try to read data in the timestamp specified. */
      com.google.cloud.Timestamp tsVal;
      try {
        tsVal = com.google.cloud.Timestamp.parseTimestamp(timestamp);
      } catch (Exception e) {
        throw new IllegalStateException("Invalid timestamp specified " + timestamp);
      }

      /*
       * If timestamp specified is in the future, spanner read will wait
       * till the time has passed. Abort the job and complain early.
       */
      if (tsVal.compareTo(com.google.cloud.Timestamp.now()) > 0) {
        throw new IllegalStateException("Timestamp specified is in future " + timestamp);
      }

      /*
       * Export jobs with Timestamps which are older than
       * maximum staleness time (one hour) fail with the FAILED_PRECONDITION
       * error - https://cloud.google.com/spanner/docs/timestamp-bounds
       * Hence we do not handle the case.
       */

      return TimestampBound.ofReadTimestamp(tsVal);
    }
  }

  /**
   * Given a list of Avro file names for each table, create the JSON string representing the
   * manifest for each table.
   */
  static class BuildTableManifests extends DoFn<KV<String, Iterable<String>>, KV<String, String>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (Objects.equals(c.element().getKey(), EMPTY_EXPORT_FILE)) {
        return;
      }
      Iterable<String> files = c.element().getValue();
      Iterator<String> it = files.iterator();
      boolean gcs = it.hasNext() && GcsPath.GCS_URI.matcher(it.next()).matches();
      TableManifest proto;
      if (gcs) {
        Iterable<GcsPath> gcsPaths = Iterables.transform(files, s -> GcsPath.fromUri(s));
        proto = buildGcsManifest(c, gcsPaths);
      } else {
        Iterable<Path> paths = Iterables.transform(files, s -> Paths.get(s));
        proto = buildLocalManifest(paths);
      }
      try {
        c.output(KV.of(c.element().getKey(), JsonFormat.printer().print(proto)));
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }

    private TableManifest buildLocalManifest(Iterable<Path> files) {
      TableManifest.Builder result = TableManifest.newBuilder();
      for (Path filePath : files) {
        String hash = FileChecksum.getLocalFileChecksum(filePath);
        result.addFilesBuilder().setName(filePath.getFileName().toString()).setMd5(hash);
      }
      return result.build();
    }

    private TableManifest buildGcsManifest(ProcessContext c, Iterable<GcsPath> files) {
      org.apache.beam.sdk.extensions.gcp.util.GcsUtil gcsUtil =
          c.getPipelineOptions().as(GcsOptions.class).getGcsUtil();
      TableManifest.Builder result = TableManifest.newBuilder();

      List<GcsPath> gcsPaths = new ArrayList<>();
      files.forEach(gcsPaths::add);

      // Fetch object metadata from GCS
      List<String> checksums = FileChecksum.getGcsFileChecksums(gcsUtil, gcsPaths);
      for (int i = 0; i < gcsPaths.size(); i++) {
        String fileName = gcsPaths.get(i).getFileName().getObject();
        String hash = checksums.get(i);
        result.addFilesBuilder().setName(fileName).setMd5(hash);
      }
      return result.build();
    }
  }
}
