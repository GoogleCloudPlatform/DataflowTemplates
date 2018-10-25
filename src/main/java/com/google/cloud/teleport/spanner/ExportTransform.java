/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.spanner;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.spanner.ExportProtos.Export;
import com.google.cloud.teleport.spanner.ExportProtos.TableManifest;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.Table;
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
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
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
import org.apache.beam.sdk.util.gcsfs.GcsPath;
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
 *  Users.avro-00000-of-00002
 *  Users.avro-00001-of-00002
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

  public ExportTransform(
      SpannerConfig spannerConfig,
      ValueProvider<String> outputDir,
      ValueProvider<String> testJobId) {
    this.spannerConfig = spannerConfig;
    this.outputDir = outputDir;
    this.testJobId = testJobId;
  }

  /**
   * Read the Cloud Spanner schema and all the rows in all the tables of the databases. Create and
   * write the exported Avro files to GCS.
   */
  @Override
  public WriteFilesResult<String> expand(PBegin begin) {
    Pipeline p = begin.getPipeline();
    PCollectionView<Transaction> tx =
        p.apply(SpannerIO.createTransaction().withSpannerConfig(spannerConfig));
    PCollection<Ddl> ddl =
        p.apply("Read Information Schema", new ReadInformationSchema(spannerConfig, tx));
    PCollection<ReadOperation> tables =
        ddl.apply("Build read operations", new BuildReadFromTableOperations());

    PCollection<KV<String, Void>> allTableNames =
        ddl.apply(
            "List all table names",
            ParDo.of(
                new DoFn<Ddl, KV<String, Void>>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Ddl ddl = c.element();
                    for (Table t : ddl.allTables()) {
                      c.output(KV.of(t.name(), null));
                    }
                  }
                }));

    // Generate a unique output directory name.
    final PCollectionView<String> outputDirectoryName =
        p.apply(Create.of(1))
            .apply(
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
                            new DdlToAvroSchemaConverter("spannerexport", "1.0.0")
                                .convert(c.element());
                        for (Schema schema : avroSchemas) {
                          c.output(KV.of(schema.getName(), new SerializableSchemaSupplier(schema)));
                        }
                      }
                    }))
            .apply("As view", View.asMap());

    PCollection<Struct> rows =
        tables.apply(
            "Read all rows from the database",
            SpannerIO.readAll().withTransaction(tx).withSpannerConfig(spannerConfig));

    ValueProvider<ResourceId> resource =
        ValueProvider.NestedValueProvider.of(
            outputDir,
            (SerializableFunction<String, ResourceId>) s -> FileSystems.matchNewResource(s, true));

    WriteFilesResult<String> fileWriteResults =
        rows.apply(
            "Store Avro files",
            AvroIO.<Struct>writeCustomTypeToGenericRecords()
                .to(new SchemaBasedDynamicDestinations(avroSchemas, outputDirectoryName, resource))
                .withTempDirectory(resource));

    // Generate the manifest file.
    PCollection<KV<String, Iterable<String>>> tableFiles =
        fileWriteResults.getPerDestinationOutputFilenames().apply(GroupByKey.create());

    final TupleTag<Void> allTables = new TupleTag<>();
    final TupleTag<Iterable<String>> nonEmptyTables = new TupleTag<>();

    PCollection<KV<String, CoGbkResult>> grouppedTables =
        KeyedPCollectionTuple.of(allTables, allTableNames)
            .and(nonEmptyTables, tableFiles)
            .apply("Group with all tables", CoGroupByKey.create());

    // The following is to export empty tables from the database.
    PCollection<KV<String, Iterable<String>>> emptyTables =
        grouppedTables.apply(
            "Empty tables",
            ParDo.of(
                new DoFn<KV<String, CoGbkResult>, KV<String, Iterable<String>>>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    KV<String, CoGbkResult> kv = c.element();
                    String table = kv.getKey();
                    CoGbkResult coGbkResult = kv.getValue();
                    Iterable<String> only = coGbkResult.getOnly(nonEmptyTables, null);
                    if (only == null) {
                      LOG.info("Exporting empty table " + table);
                      c.output(KV.of(table, Collections.singleton(table + ".avro-00000-of-00001")));
                    }
                  }
                }));

    emptyTables =
        emptyTables.apply(
            "Save empty schema files",
            ParDo.of(
                    new DoFn<KV<String, Iterable<String>>, KV<String, Iterable<String>>>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        Map<String, SerializableSchemaSupplier> schemaMap =
                            c.sideInput(avroSchemas);
                        KV<String, Iterable<String>> kv = c.element();
                        String tableName = kv.getKey();
                        String fileName = kv.getValue().iterator().next();

                        Schema schema = schemaMap.get(tableName).get();
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
                        c.output(KV.of(tableName, Collections.singleton(fullPath.toString())));
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
                          org.apache.beam.sdk.util.GcsUtil gcsUtil =
                              c.getPipelineOptions().as(GcsOptions.class).getGcsUtil();
                          String gcsType = "application/octet-stream";
                          WritableByteChannel gcsChannel =
                              gcsUtil.create((GcsPath) outputPath, gcsType);
                          return Channels.newOutputStream(gcsChannel);
                        } else {
                          // Avro file is created on local filesystem (for testing).
                          return java.nio.file.Files.newOutputStream(outputPath);
                        }
                      }
                    })
                .withSideInputs(avroSchemas, outputDirectoryName));

    PCollection<KV<String, Iterable<String>>> allFiles =
        PCollectionList.of(tableFiles)
            .and(emptyTables)
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
        "Store contents",
        FileIO.<String, KV<String, String>>writeDynamic()
            .by(KV::getKey)
            .withDestinationCoder(StringUtf8Coder.of())
            .withNaming(
                Contextful.of(
                    tableManifestNaming, Requirements.requiresSideInputs(outputDirectoryName)))
            .via(Contextful.fn(KV::getValue), TextIO.sink())
            .withTempDirectory(outputDir));

    PCollection<String> metadataContent =
        tableManifests.apply(
            "Create database manifest", Combine.globally(new CreateDatabaseManifest()));

    Contextful.Fn<String, FileIO.Write.FileNaming> manifestNaming =
        (element, c) ->
            (window, pane, numShards, shardIndex, compression) ->
                GcsUtil.joinPath(
                    outputDir.get(), c.sideInput(outputDirectoryName), "spanner-export.json");

    metadataContent.apply(
        "Store the manifest file",
        FileIO.<String, String>writeDynamic()
            .by(SerializableFunctions.constant(""))
            .withDestinationCoder(StringUtf8Coder.of())
            .via(TextIO.sink())
            .withTempDirectory(outputDir)
            .withNaming(
                Contextful.of(
                    manifestNaming, Requirements.requiresSideInputs(outputDirectoryName))));
    return fileWriteResults;
  }

  /**
   * Saves {@link Struct} elements of PCollection of the same type to the an Avro file.
   */
  private static class SchemaBasedDynamicDestinations
      extends DynamicAvroDestinations<Struct, String, GenericRecord> {

    private final PCollectionView<Map<String, SerializableSchemaSupplier>> avroSchemas;
    private final PCollectionView<String> uniqueIdView;
    private final ValueProvider<ResourceId> baseDir;

    private SchemaBasedDynamicDestinations(
        PCollectionView<Map<String, SerializableSchemaSupplier>> avroSchemas,
        PCollectionView<String> uniqueIdView,
        ValueProvider<ResourceId> baseDir) {
      this.avroSchemas = avroSchemas;
      this.uniqueIdView = uniqueIdView;
      this.baseDir = baseDir;
    }

    @Override
    public Schema getSchema(String tableName) {
      Map<String, SerializableSchemaSupplier> si = sideInput(avroSchemas);
      // Check if there are any schemas available.
      if (si.isEmpty()) {
        // The EMPTY_EXPORT_FILE still needs to have a rudimentary schema for it to be created.
        return SchemaBuilder.record("EmptyDB").fields().endRecord();
      }
      return si.get(tableName).get();
    }

    @Override
    public String getDestination(Struct element) {
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
      return Arrays.asList(avroSchemas, uniqueIdView);
    }

    @Override
    public GenericRecord formatRecord(Struct record) {
      String table = record.getString(0);
      Schema schema = sideInput(avroSchemas).get(table).get();
      return new SpannerRecordConverter(schema).convert(record);
    }
  }

  // TODO: use AvroUtils.serializableSchemaSupplier once it is public
  private static class SerializableSchemaString implements Serializable {

    private final String schema;

    private SerializableSchemaString(String schema) {
      this.schema = schema;
    }

    private Object readResolve() throws IOException, ClassNotFoundException {
      return new SerializableSchemaSupplier(Schema.parse(schema));
    }
  }

  private static class SerializableSchemaSupplier implements Serializable, Supplier<Schema> {

    private final Schema schema;

    private SerializableSchemaSupplier(Schema schema) {
      this.schema = schema;
    }

    private Object writeReplace() {
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
  static class CreateDatabaseManifest
      extends CombineFn<KV<String, String>, List<Export.Table>, String> {

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
    public String extractOutput(List<Export.Table> accumulator) {
      ExportProtos.Export.Builder exportManifest = ExportProtos.Export.newBuilder();
      exportManifest.addAllTables(accumulator);
      try {
        return JsonFormat.printer().print(exportManifest.build());
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static String tableManifestFileName(String tableName) {
    return tableName + "-manifest.json";
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
      org.apache.beam.sdk.util.GcsUtil gcsUtil =
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
