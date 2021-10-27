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

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.spanner.ExportProtos.Export;
import com.google.cloud.teleport.spanner.ExportProtos.TableManifest;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.Table;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Verify;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.protobuf.util.JsonFormat;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.spanner.ExposedSpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.LocalSpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Beam transform that imports a Cloud Spanner database from a set of Avro files. */
public class ImportTransform extends PTransform<PBegin, PDone> {

  private static final Logger LOG = LoggerFactory.getLogger(ImportTransform.class);
  private static final int MAX_DEPTH = 8;

  private final SpannerConfig spannerConfig;
  private final ValueProvider<String> importDirectory;
  // By default the import pipeline is not blocked on index or foreign key creation, and it
  // may complete with indexes or foreign keys still being created in the background. In testing,
  // it may be useful to wait until indexes and foreign keys are finished for verification.
  // If the following two fields are true, the transform will wait until indexes and foreign keys
  // are finished, respectively.
  private final ValueProvider<Boolean> waitForIndexes;
  private final ValueProvider<Boolean> waitForForeignKeys;
  private final ValueProvider<Boolean> earlyIndexCreateFlag;

  public ImportTransform(
      SpannerConfig spannerConfig,
      ValueProvider<String> importDirectory,
      ValueProvider<Boolean> waitForIndexes,
      ValueProvider<Boolean> waitForForeignKeys,
      ValueProvider<Boolean> earlyIndexCreateFlag) {
    this.spannerConfig = spannerConfig;
    this.importDirectory = importDirectory;
    this.waitForIndexes = waitForIndexes;
    this.waitForForeignKeys = waitForForeignKeys;
    this.earlyIndexCreateFlag = earlyIndexCreateFlag;
  }

  @Override
  public PDone expand(PBegin begin) {
    PCollection<Export> manifest =
        begin.apply("Read manifest", new ReadExportManifestFile(importDirectory));
    PCollectionView<Export> manifestView = manifest.apply("Manifest as view", View.asSingleton());

    PCollection<KV<String, String>> allFiles =
        manifest.apply("Read all manifest files", new ReadManifestFiles(importDirectory));

    PCollection<KV<String, List<String>>> tableFiles = allFiles.apply(Combine.perKey(AsList.fn()));

    PCollection<KV<String, String>> schemas =
        tableFiles
            .apply(
                "File per table",
                ParDo.of(
                    new DoFn<KV<String, List<String>>, KV<String, String>>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        KV<String, List<String>> kv = c.element();
                        if (!kv.getValue().isEmpty()) {
                          c.output(KV.of(kv.getKey(), kv.getValue().get(0)));
                        }
                      }
                    }))
            .apply("Extract avro schemas", ParDo.of(new ReadAvroSchemas()));

    final PCollection<List<KV<String, String>>> avroSchemas =
        schemas.apply("Build avro DDL", Combine.globally(AsList.fn()));

    PCollectionView<Transaction> tx =
        begin.apply(LocalSpannerIO.createTransaction().withSpannerConfig(spannerConfig));

    PCollection<Ddl> informationSchemaDdl =
        begin.apply("Read Information Schema", new ReadInformationSchema(spannerConfig, tx));

    final PCollectionView<List<KV<String, String>>> avroDdlView =
        avroSchemas.apply("Avro ddl view", View.asSingleton());
    final PCollectionView<Ddl> informationSchemaView =
        informationSchemaDdl.apply("Information schema view", View.asSingleton());
    final PCollectionTuple createTableOutput =
        begin.apply(
            "Create Cloud Spanner Tables and indexes",
            new CreateTables(
                spannerConfig,
                avroDdlView,
                informationSchemaView,
                manifestView,
                earlyIndexCreateFlag));

    final PCollection<Ddl> ddl = createTableOutput.get(CreateTables.getDdlObjectTag());
    final PCollectionView<List<String>> pendingIndexes =
        createTableOutput
            .get(CreateTables.getPendingIndexesTag())
            .apply("As Index view", View.asSingleton());
    final PCollectionView<List<String>> pendingForeignKeys =
        createTableOutput
            .get(CreateTables.getPendingForeignKeysTag())
            .apply("As Foreign keys view", View.asSingleton());

    PCollectionView<Ddl> ddlView = ddl.apply("Cloud Spanner DDL as view", View.asSingleton());

    PCollectionView<HashMultimap<Integer, String>> levelsView =
        ddl.apply(
                "Group tables by depth",
                ParDo.of(
                    new DoFn<Ddl, HashMultimap<Integer, String>>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        Ddl ddl = c.element();
                        c.output(ddl.perLevelView());
                      }
                    }))
            .apply(View.asSingleton());

    PCollection<HashMultimap<String, String>> acc =
        tableFiles
            .apply("Combine table files", Combine.globally(AsList.fn()))
            .apply(
                "As HashMultimap",
                ParDo.of(
                    new DoFn<List<KV<String, List<String>>>, HashMultimap<String, String>>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        HashMultimap<String, String> result = HashMultimap.create();
                        for (KV<String, List<String>> kv : c.element()) {
                          result.putAll(kv.getKey().toLowerCase(), kv.getValue());
                        }
                        c.output(result);
                      }
                    }));

    PCollection<?> previousComputation = ddl;
    for (int i = 0; i < MAX_DEPTH; i++) {
      final int depth = i;
      PCollection<KV<String, String>> levelFiles =
          acc.apply(
                  "Get Avro filenames depth " + depth,
                  ParDo.of(
                          new DoFn<HashMultimap<String, String>, KV<String, String>>() {

                            @ProcessElement
                            public void processElement(ProcessContext c) {
                              HashMultimap<String, String> allFiles = c.element();
                              HashMultimap<Integer, String> levels = c.sideInput(levelsView);

                              Set<String> tables = levels.get(depth);
                              for (String table : tables) {
                                for (String file : allFiles.get(table)) {
                                  c.output(KV.of(file, table));
                                }
                              }
                            }
                          })
                      .withSideInputs(levelsView))
              .apply("Wait for previous depth " + depth, Wait.on(previousComputation));
      PCollection<Mutation> mutations =
          levelFiles.apply(
              "Avro files as mutations " + depth, new AvroTableFileAsMutations(ddlView));

      SpannerWriteResult result =
          mutations.apply(
              "Write mutations " + depth,
              LocalSpannerIO.write()
                  .withSchemaReadySignal(ddl)
                  .withSpannerConfig(spannerConfig)
                  .withCommitDeadline(Duration.standardMinutes(1))
                  .withMaxCumulativeBackoff(Duration.standardHours(2))
                  .withMaxNumMutations(10000)
                  .withGroupingFactor(100));
      previousComputation = result.getOutput();
    }
    ddl.apply(Wait.on(previousComputation))
        .apply(
            "Create Indexes", new ApplyDDLTransform(spannerConfig, pendingIndexes, waitForIndexes))
        .apply(
            "Add Foreign Keys",
            new ApplyDDLTransform(spannerConfig, pendingForeignKeys, waitForForeignKeys));
    return PDone.in(begin.getPipeline());
  }

  /** Read contents of the top-level manifest file. */
  private static class ReadExportManifestFile extends PTransform<PBegin, PCollection<Export>> {

    private final ValueProvider<String> importDirectory;

    private ReadExportManifestFile(ValueProvider<String> importDirectory) {
      this.importDirectory = importDirectory;
    }

    @Override
    public PCollection<Export> expand(PBegin input) {
      NestedValueProvider<String, String> manifestFile =
          NestedValueProvider.of(importDirectory, s -> GcsUtil.joinPath(s, "spanner-export.json"));
      return input
          .apply("Read manifest", FileIO.match().filepattern(manifestFile))
          .apply(
              "Resource id",
              MapElements.into(TypeDescriptor.of(ResourceId.class))
                  .via((MatchResult.Metadata::resourceId)))
          .apply(
              "Read manifest json",
              MapElements.into(TypeDescriptor.of(Export.class))
                  .via(ReadExportManifestFile::readManifest));
    }

    private static Export readManifest(ResourceId fileResource) {
      Export.Builder result = Export.newBuilder();
      try (InputStream stream = Channels.newInputStream(FileSystems.open(fileResource))) {
        Reader reader = new InputStreamReader(stream);
        JsonFormat.parser().merge(reader, result);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return result.build();
    }
  }

  private static class ReadTableManifestFile
      extends PTransform<PCollection<KV<String, String>>, PCollection<KV<String, TableManifest>>> {

    private final ValueProvider<String> importDirectory;

    private ReadTableManifestFile(ValueProvider<String> importDirectory) {
      this.importDirectory = importDirectory;
    }

    @Override
    public PCollection<KV<String, TableManifest>> expand(PCollection<KV<String, String>> input) {
      return input.apply(
          "Read table manifest",
          ParDo.of(
              new DoFn<KV<String, String>, KV<String, TableManifest>>() {

                @ProcessElement
                public void processElement(ProcessContext c) {
                  try {
                    KV<String, String> kv = c.element();
                    String filePath = GcsUtil.joinPath(importDirectory.get(), kv.getValue());
                    MatchResult match = FileSystems.match(filePath, EmptyMatchTreatment.DISALLOW);
                    ResourceId resourceId = match.metadata().get(0).resourceId();
                    TableManifest.Builder builder = TableManifest.newBuilder();
                    try (InputStream stream =
                        Channels.newInputStream(FileSystems.open(resourceId))) {
                      Reader reader = new InputStreamReader(stream);
                      JsonFormat.parser().merge(reader, builder);
                    }
                    c.output(KV.of(kv.getKey(), builder.build()));
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                }
              }));
    }
  }

  private static class CreateTables extends PTransform<PBegin, PCollectionTuple> {

    private final SpannerConfig spannerConfig;

    private final PCollectionView<List<KV<String, String>>> avroSchemasView;
    private final PCollectionView<Ddl> informationSchemaView;
    private final PCollectionView<Export> manifestView;
    private final ValueProvider<Boolean> earlyIndexCreateFlag;

    private transient ExposedSpannerAccessor spannerAccessor;

    /* If the schema has a lot of DDL changes after dataload, its preferable to create
     * them before dataload. This provides the threshold for the early creation.
     */
    private static final int EARLY_INDEX_CREATE_THRESHOLD = 40;

    public static TupleTag<Ddl> getDdlObjectTag() {
      return ddlObjectTag;
    }

    public static TupleTag<List<String>> getPendingIndexesTag() {
      return pendingIndexesTag;
    }

    public static TupleTag<List<String>> getPendingForeignKeysTag() {
      return pendingForeignKeysTag;
    }

    private static final TupleTag<Ddl> ddlObjectTag = new TupleTag<Ddl>() {};
    private static final TupleTag<List<String>> pendingIndexesTag = new TupleTag<List<String>>() {};
    private static final TupleTag<List<String>> pendingForeignKeysTag =
        new TupleTag<List<String>>() {};

    public CreateTables(
        SpannerConfig spannerConfig,
        PCollectionView<List<KV<String, String>>> avroSchemasView,
        PCollectionView<Ddl> informationSchemaView,
        PCollectionView<Export> manifestView,
        ValueProvider<Boolean> earlyIndexCreateFlag) {
      this.spannerConfig = spannerConfig;
      this.avroSchemasView = avroSchemasView;
      this.informationSchemaView = informationSchemaView;
      this.manifestView = manifestView;
      this.earlyIndexCreateFlag = earlyIndexCreateFlag;
    }

    @Override
    public PCollectionTuple expand(PBegin begin) {
      return begin
          .apply(Create.of(1))
          .apply(
              ParDo.of(
                      new DoFn<Integer, Ddl>() {

                        @Setup
                        public void setup() {
                          spannerAccessor = ExposedSpannerAccessor.create(spannerConfig);
                        }

                        @Teardown
                        public void teardown() {
                          spannerAccessor.close();
                        }

                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          List<KV<String, String>> avroSchemas = c.sideInput(avroSchemasView);
                          Ddl informationSchemaDdl = c.sideInput(informationSchemaView);
                          Export manifest = c.sideInput(manifestView);

                          if (LOG.isDebugEnabled()) {
                            LOG.debug(informationSchemaDdl.prettyPrint());
                          }
                          Schema.Parser parser = new Schema.Parser();
                          List<KV<String, Schema>> missingTables = new ArrayList<>();
                          List<KV<String, Schema>> missingViews = new ArrayList<>();
                          for (KV<String, String> kv : avroSchemas) {
                            if (informationSchemaDdl.table(kv.getKey()) == null
                                && informationSchemaDdl.view(kv.getKey()) == null) {
                              Schema schema = parser.parse(kv.getValue());
                              if (schema.getProp("spannerViewQuery") != null) {
                                missingViews.add(KV.of(kv.getKey(), schema));
                              } else {
                                missingTables.add(KV.of(kv.getKey(), schema));
                              }
                            }
                          }
                          AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter();
                          List<String> createIndexStatements = new ArrayList<>();
                          List<String> createForeignKeyStatements = new ArrayList<>();

                          Ddl.Builder mergedDdl = informationSchemaDdl.toBuilder();
                          List<String> ddlStatements = new ArrayList<>();

                          if (!manifest.getDatabaseOptionsList().isEmpty()) {
                            Ddl.Builder builder = Ddl.builder();
                            builder.mergeDatabaseOptions(manifest.getDatabaseOptionsList());
                            mergedDdl.mergeDatabaseOptions(manifest.getDatabaseOptionsList());
                            Ddl newDdl = builder.build();
                            ddlStatements.addAll(
                                newDdl.setOptionsStatements(spannerConfig.getDatabaseId().get()));
                          }

                          if (!missingTables.isEmpty() || !missingViews.isEmpty()) {
                            Ddl.Builder builder = Ddl.builder();
                            for (KV<String, Schema> kv : missingViews) {
                              com.google.cloud.teleport.spanner.ddl.View view =
                                  converter.toView(kv.getKey(), kv.getValue());
                              builder.addView(view);
                              mergedDdl.addView(view);
                            }
                            for (KV<String, Schema> kv : missingTables) {
                              Table table = converter.toTable(kv.getKey(), kv.getValue());
                              builder.addTable(table);
                              mergedDdl.addTable(table);
                              // Account for additional DDL changes for tables being created
                              createIndexStatements.addAll(table.indexes());
                              createForeignKeyStatements.addAll(table.foreignKeys());
                            }
                            Ddl newDdl = builder.build();
                            ddlStatements.addAll(newDdl.createTableStatements());
                            ddlStatements.addAll(newDdl.createViewStatements());
                            // If the total DDL statements exceed the threshold, execute the create
                            // index statements when tables are created.
                            // Note that foreign keys can only be created after data load
                            // because if we tried to create them before data load, we would
                            // need to load rows in a specific order (insert the referenced
                            // row first before the referencing row). This is not always
                            // possible since foreign keys may introduce circular relationships.
                            if (earlyIndexCreateFlag.get()
                                && ((createForeignKeyStatements.size()
                                        + createIndexStatements.size())
                                    >= EARLY_INDEX_CREATE_THRESHOLD)) {
                              ddlStatements.addAll(createIndexStatements);
                              c.output(pendingIndexesTag, new ArrayList<String>());
                            } else {
                              c.output(pendingIndexesTag, createIndexStatements);
                            }
                            c.output(pendingForeignKeysTag, createForeignKeyStatements);
                          }

                          if (!ddlStatements.isEmpty()) {
                            DatabaseAdminClient databaseAdminClient =
                                spannerAccessor.getDatabaseAdminClient();
                            OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
                                databaseAdminClient.updateDatabaseDdl(
                                    spannerConfig.getInstanceId().get(),
                                    spannerConfig.getDatabaseId().get(),
                                    ddlStatements,
                                    null);
                            try {
                              // TODO: Wait till operation is complete.
                              op.get(30, TimeUnit.MINUTES);
                            } catch (InterruptedException
                                | ExecutionException
                                | TimeoutException e) {
                              throw new RuntimeException(e);
                            }
                            c.output(mergedDdl.build());
                          } else {
                            c.output(informationSchemaDdl);
                          }
                          // In case of no tables, add empty list
                          if (missingTables.isEmpty()) {
                            c.output(pendingIndexesTag, createIndexStatements);
                            c.output(pendingForeignKeysTag, createForeignKeyStatements);
                          }
                        }
                      })
                  .withSideInputs(avroSchemasView, informationSchemaView, manifestView)
                  .withOutputTags(
                      ddlObjectTag, TupleTagList.of(pendingIndexesTag).and(pendingForeignKeysTag)));
    }
  }

  private static class ReadAvroSchemas extends DoFn<KV<String, String>, KV<String, String>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<String, String> kv = c.element();

      String schema = null;
      ResourceId resourceId = FileSystems.matchNewResource(kv.getValue(), false);
      try (InputStream stream = Channels.newInputStream(FileSystems.open(resourceId))) {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(stream, null);
        byte[] magic = new byte[DataFileConstants.MAGIC.length];
        decoder.readFixed(magic);
        if (!Arrays.equals(magic, DataFileConstants.MAGIC)) {
          throw new IOException("Missing Avro file signature: " + kv.getValue());
        }

        // Read the metadata to find the codec and schema.
        ByteBuffer valueBuffer = ByteBuffer.allocate(512);
        long numRecords = decoder.readMapStart();
        while (numRecords > 0 && schema == null) {
          for (long recordIndex = 0; recordIndex < numRecords; recordIndex++) {
            String key = decoder.readString();
            // readBytes() clears the buffer and returns a buffer where:
            // - position is the start of the bytes read
            // - limit is the end of the bytes read
            valueBuffer = decoder.readBytes(valueBuffer);
            byte[] bytes = new byte[valueBuffer.remaining()];
            valueBuffer.get(bytes);
            if (key.equals(DataFileConstants.SCHEMA)) {
              schema = new String(bytes, "UTF-8");
              break;
            }
          }
          numRecords = decoder.mapNext();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      c.output(KV.of(kv.getKey(), schema));
    }
  }

  /** Expands {@link Export} as a PCollection of (table, file) pairs that should be imported. */
  public static class ReadManifestFiles
      extends PTransform<PCollection<Export>, PCollection<KV<String, String>>> {
    private final ValueProvider<String> importDirectory;

    public ReadManifestFiles(ValueProvider<String> importDirectory) {
      this.importDirectory = importDirectory;
    }

    @Override
    public PCollection<KV<String, String>> expand(PCollection<Export> manifest) {
      PCollection<KV<String, String>> dataFiles =
          manifest.apply(
              "Extract data files",
              ParDo.of(
                  new DoFn<Export, KV<String, String>>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      Export proto = c.element();

                      for (Export.Table table : proto.getTablesList()) {
                        for (String f : table.getDataFilesList()) {
                          String fullPath = GcsUtil.joinPath(importDirectory.get(), f);
                          c.output(KV.of(table.getName(), fullPath));
                        }
                      }
                    }
                  }));

      PCollection<KV<String, String>> manifestFiles =
          manifest.apply(
              "Extract manifest files",
              ParDo.of(
                  new DoFn<Export, KV<String, String>>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      Export proto = c.element();
                      for (Export.Table table : proto.getTablesList()) {
                        if (!Strings.isNullOrEmpty(table.getManifestFile())) {
                          c.output(KV.of(table.getName(), table.getManifestFile()));
                        }
                      }
                    }
                  }));

      PCollection<KV<String, TableManifest>> manifests =
          manifestFiles.apply(
              "Read table manifest contents", new ReadTableManifestFile(importDirectory));

      PCollection<KV<String, String>> expandedFromManifests =
          manifests.apply(
              "Validate input files", ParDo.of(new ValidateInputFiles(importDirectory)));

      return PCollectionList.of(dataFiles).and(expandedFromManifests).apply(Flatten.pCollections());
    }
  }

  /**
   * Find checksums for the input files and validate against checksums in the manifests. Returns
   * multi-map of input files for each table.
   */
  @VisibleForTesting
  static class ValidateInputFiles extends DoFn<KV<String, TableManifest>, KV<String, String>> {

    private final ValueProvider<String> importDirectory;

    ValidateInputFiles(ValueProvider<String> importDirectory) {
      this.importDirectory = importDirectory;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<String, TableManifest> kv = c.element();
      String table = kv.getKey();
      TableManifest manifest = kv.getValue();
      boolean gcs = GcsPath.GCS_URI.matcher(importDirectory.get()).matches();
      if (gcs) {
        validateGcsFiles(c, table, manifest);
      } else {
        validateLocalFiles(c, table, manifest);
      }
    }

    private void validateGcsFiles(ProcessContext c, String table, TableManifest manifest) {
      org.apache.beam.sdk.extensions.gcp.util.GcsUtil gcsUtil =
          c.getPipelineOptions().as(GcsOptions.class).getGcsUtil();
      // Convert file names to GcsPaths.
      List<GcsPath> gcsPaths =
          Lists.transform(
              manifest.getFilesList(),
              f -> GcsPath.fromUri(importDirectory.get()).resolve(f.getName()));
      List<String> checksums = FileChecksum.getGcsFileChecksums(gcsUtil, gcsPaths);
      for (int i = 0; i < gcsPaths.size(); i++) {
        GcsPath path = gcsPaths.get(i);
        String fileName = gcsPaths.get(i).getFileName().getObject();
        String expectedHash = manifest.getFiles(i).getMd5();
        String actualHash = checksums.get(i);
        Verify.verify(
            expectedHash.equals(actualHash),
            "Inconsistent file: %s expected hash %s actual hash %s",
            fileName,
            expectedHash,
            actualHash);
        c.output(KV.of(table, path.toString()));
      }
    }

    private void validateLocalFiles(ProcessContext c, String table, TableManifest manifest) {
      for (TableManifest.File file : manifest.getFilesList()) {
        Path filePath = Paths.get(importDirectory.get(), file.getName());
        String actualHash = FileChecksum.getLocalFileChecksum(filePath);
        String expectedHash = file.getMd5();
        Verify.verify(
            expectedHash.equals(actualHash),
            "Inconsistent file: %s expected hash %s actual hash %s",
            filePath,
            expectedHash,
            actualHash);
        c.output(KV.of(table, filePath.toString()));
      }
    }
  }
}
