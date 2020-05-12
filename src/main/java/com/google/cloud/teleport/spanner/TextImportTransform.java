/*
 * Copyright (C) 2019 Google Inc.
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

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type.Code;
import com.google.cloud.teleport.spanner.TextImportProtos.ImportManifest;
import com.google.cloud.teleport.spanner.TextImportProtos.ImportManifest.TableManifest;
import com.google.cloud.teleport.spanner.ddl.Column;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.Table;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Beam transform that imports a set of text files to a Cloud Spanner database. */
public class TextImportTransform extends PTransform<PBegin, PDone> {

  private static final Logger LOG = LoggerFactory.getLogger(ImportTransform.class);
  private static final int MAX_DEPTH = 8;

  private final SpannerConfig spannerConfig;

  private final ValueProvider<String> importManifest;

  public TextImportTransform(SpannerConfig spannerConfig, ValueProvider<String> importManifest) {
    this.spannerConfig = spannerConfig;
    this.importManifest = importManifest;
  }

  @Override
  public PDone expand(PBegin begin) {
    PCollectionView<Transaction> tx =
        begin.apply(SpannerIO.createTransaction().withSpannerConfig(spannerConfig));

    PCollection<Ddl> ddl =
        begin.apply("Read Information Schema", new ReadInformationSchema(spannerConfig, tx));

    PCollectionView<Ddl> ddlView = ddl.apply("Cloud Spanner DDL as view", View.asSingleton());

    PCollection<ImportManifest> manifest =
        begin.apply("Read manifest file", new ReadImportManifest(importManifest));

    PCollection<KV<String, String>> allFiles =
        manifest.apply("Resolve data files", new ResolveDataFiles(importManifest, ddlView));

    PCollection<Map<String, List<TableManifest.Column>>> tableColumns =
        manifest.apply("Read table columns from manifest", new ReadTableColumns());

    PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsView =
        tableColumns.apply("tableColumns as View", View.asSingleton());

    PCollection<KV<String, List<String>>> tableFiles = allFiles.apply(Combine.perKey(AsList.fn()));

    // TODO: add a step to check that schema in the manifest match db schema.
    PCollection<HashMultimap<Integer, String>> levelMap =
        ddl.apply(
            "Group tables by depth",
            ParDo.of(
                new DoFn<Ddl, HashMultimap<Integer, String>>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Ddl ddl = c.element();
                    c.output(ddl.perLevelView());
                  }
                }));

    PCollectionView<HashMultimap<Integer, String>> levelsView =
        levelMap.apply("Level map as view", View.asSingleton());

    PCollection<HashMultimap<String, String>> tablesToFilesMap =
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
      PCollection<KV<String, String>> levelFileToTables =
          tablesToFilesMap.apply(
              "Store depth " + depth,
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
                  .withSideInputs(levelsView));

      PCollection<Mutation> mutations =
          levelFileToTables
              .apply("Reshuffle text files " + depth, Reshuffle.viaRandomKey())
              .apply(
                  "Text files as mutations. Depth: " + depth,
                  new TextTableFilesAsMutations(ddlView, tableColumnsView));

      SpannerWriteResult result =
          mutations
              .apply("Wait for previous depth " + depth, Wait.on(previousComputation))
              .apply(
                  "Write mutations " + depth, SpannerIO.write().withSpannerConfig(spannerConfig)
                      .withCommitDeadline(Duration.standardMinutes(1))
                      .withMaxCumulativeBackoff(Duration.standardHours(2))
                      .withMaxNumMutations(10000)
                      .withGroupingFactor(100));
      previousComputation = result.getOutput();
    }

    return PDone.in(begin.getPipeline());
  }

  /** A transform that converts CSV records to Cloud Spanner mutations. */
  private class TextTableFilesAsMutations
      extends PTransform<PCollection<KV<String, String>>, PCollection<Mutation>> {

    private final PCollectionView<Ddl> ddlView;
    private final PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsView;

    public TextTableFilesAsMutations(
        PCollectionView<Ddl> ddlView,
        PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsView) {
      this.ddlView = ddlView;
      this.tableColumnsView = tableColumnsView;
    }

    @Override
    public PCollection<Mutation> expand(PCollection<KV<String, String>> filesToTables) {
      // Map<filename,tablename>
      PCollectionView<Map<String, String>> filesToTablesMapView =
          filesToTables.apply("asView", View.asMap());
      TextImportPipeline.Options options =
          filesToTables.getPipeline().getOptions().as(TextImportPipeline.Options.class);

      return filesToTables
          .apply("Get Filenames", Keys.create())
          // PCollection<String>
          .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
          // PCollection<Match.Metadata>
          .apply(FileIO.readMatches())
          // PCollection<FileIO.ReadableFile>
          .apply(
              "Split into ranges",
              ParDo.of(
                      new SplitIntoRangesFn(
                          SplitIntoRangesFn.DEFAULT_BUNDLE_SIZE, filesToTablesMapView))
                  .withSideInputs(filesToTablesMapView))
          .setCoder(FileShard.Coder.of())
          // PCollection<FileShard>
          .apply("Reshuffle", Reshuffle.viaRandomKey())
          // PCollection<FileShard>
          .apply(
              "Read lines",
              ParDo.of(
                  new DoFn<FileShard, KV<String, String>>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      FileShard shard = c.element();

                      // Create a TextSource, passing null as the delimiter to use the default
                      // delimiters ('\n', '\r', or '\r\n').
                      TextSource textSource =
                          new TextSource(
                              shard.getFile().getMetadata(),
                              shard.getRange().getFrom(),
                              shard.getRange().getTo(),
                              null);
                      String line;
                      try {
                        BoundedSource.BoundedReader<String> reader =
                            textSource
                                .createForSubrangeOfFile(
                                    shard.getFile().getMetadata(),
                                    shard.getRange().getFrom(),
                                    shard.getRange().getTo())
                                .createReader(c.getPipelineOptions());
                        for (boolean more = reader.start(); more; more = reader.advance()) {
                          c.output(KV.of(shard.getTableName(), reader.getCurrent()));
                        }
                      } catch (IOException e) {
                        throw new RuntimeException(
                            "Unable to readFile: "
                                + shard.getFile().getMetadata().resourceId().toString());
                      }
                    }
                  }))
          // PCollection<KV<String, String>>: tableName, line
          .apply(
              ParDo.of(
                      new TextRowToMutation(
                          ddlView,
                          tableColumnsView,
                          options.getColumnDelimiter(),
                          options.getFieldQualifier(),
                          options.getTrailingDelimiter(),
                          options.getEscape(),
                          options.getNullString(),
                          options.getDateFormat(),
                          options.getTimestampFormat()))
                  .withSideInputs(ddlView, tableColumnsView));
    }
  }

  /**
   * Read contents of the import manifest file, which is a json file with the following format: [ {
   * "table": "table_1", "files": [ "table_1_data_file_1", "table_1_data_file_2",
   * "table_1_data_file_3" ] }, { "table": "table_2", "files": [ "table_2_data_file*"] } ]. When
   * using GLOB patterns in the files field, please make sure the patterns match input files
   * properly. Take TPC-H benchmark data files as an example, where input files include part.tbl.1,
   * part.tbl.2, partsupp.tbl.1, and partsupp.tbl.2. Table PARTSUPP can use pattern "partsupp*".
   * However, table PART cannot use "part*", as it will incorrectly match file partsupp.tbl.1 as
   * well. Using a more specific pattern such as "part.tbl.*" will solve the issue.
   */
  @VisibleForTesting
  static class ReadImportManifest extends PTransform<PBegin, PCollection<ImportManifest>> {

    private final ValueProvider<String> importManifest;

    ReadImportManifest(ValueProvider<String> importManifest) {
      this.importManifest = importManifest;
    }

    @Override
    public PCollection<ImportManifest> expand(PBegin input) {
      return input
          .apply("Read manifest", FileIO.match().filepattern(importManifest))
          .apply(
              "Resource id",
              MapElements.into(TypeDescriptor.of(ResourceId.class))
                  .via((MatchResult.Metadata::resourceId)))
          .apply(
              "Read manifest json",
              MapElements.into(TypeDescriptor.of(ImportManifest.class))
                  .via(ReadImportManifest::readManifest));
    }

    private static ImportManifest readManifest(ResourceId fileResource) {
      ImportManifest.Builder result = ImportManifest.newBuilder();
      try (InputStream stream = Channels.newInputStream(FileSystems.open(fileResource))) {
        Reader reader = new InputStreamReader(stream, StandardCharsets.UTF_8);
        JsonFormat.parser().merge(reader, result);
      } catch (IOException e) {
        throw new RuntimeException(
            "Failed to read manifest. Make sure it is ASCII or UTF-8 encoded and contains a"
                + " well-formed JSON string. Please refer to"
                + " https://cloud.google.com/spanner/docs/import-export-csv#create-json-manifest"
                + " for the required format of the manifest file.",
            e);
      }
      return result.build();
    }
  }

  @VisibleForTesting
  static class ResolveDataFiles
      extends PTransform<PCollection<ImportManifest>, PCollection<KV<String, String>>> {

    private final ValueProvider<String> importManifest;
    private final PCollectionView<Ddl> ddlView;

    ResolveDataFiles(ValueProvider<String> importManifest, PCollectionView<Ddl> ddlView) {
      this.importManifest = importManifest;
      this.ddlView = ddlView;
    }

    @Override
    public PCollection<KV<String, String>> expand(PCollection<ImportManifest> input) {

      return input.apply(
          "Resolve manifest to table name and file name",
          ParDo.of(
                  new DoFn<ImportManifest, KV<String, String>>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      GcsUtil gcsUtil = c.getPipelineOptions().as(GcsOptions.class).getGcsUtil();
                      ImportManifest manifest = c.element();
                      Ddl ddl = c.sideInput(ddlView);
                      boolean isGcs = GcsPath.GCS_URI.matcher(importManifest.get()).matches();

                      for (ImportManifest.TableManifest tableManifest : manifest.getTablesList()) {
                        validateManifest(tableManifest, ddl);
                        for (String pattern : tableManifest.getFilePatternsList()) {
                          try {
                            if (isGcs) {
                              gcsUtil
                                  .expand(GcsPath.fromUri(pattern))
                                  .forEach(
                                      path ->
                                          c.output(
                                              KV.of(
                                                  tableManifest.getTableName().toLowerCase(),
                                                  path.toString())));
                            } else {
                              File file = new File(pattern);
                              String parent = file.getParent();
                              if (parent != null) {
                                DirectoryStream<Path> matchingFiles =
                                    Files.newDirectoryStream(
                                        Paths.get(file.getParent()), file.getName());
                                for (Path p : matchingFiles) {
                                  c.output(
                                      KV.of(
                                          tableManifest.getTableName().toLowerCase(),
                                          p.toString()));
                                }
                              }
                            }
                          } catch (IOException e) {
                            throw new RuntimeException(e);
                          }
                        }
                      }
                    }
                  })
              .withSideInputs(ddlView));
    }

    public static Code parseSpannerDataType(String columnType) {
      if (columnType.matches("STRING(?:\\((?:MAX|[0-9]+)\\))?")) {
        return Code.STRING;
      } else if (columnType.equalsIgnoreCase("INT64")) {
        return Code.INT64;
      } else if (columnType.equalsIgnoreCase("FLOAT64")) {
        return Code.FLOAT64;
      } else if (columnType.equalsIgnoreCase("BOOL")) {
        return Code.BOOL;
      } else if (columnType.equalsIgnoreCase("DATE")) {
        return Code.DATE;
      } else if (columnType.equalsIgnoreCase("TIMESTAMP")) {
        return Code.TIMESTAMP;
      } else if (columnType.equalsIgnoreCase("BYTES")) {
        return Code.BYTES;
      } else {
        throw new IllegalArgumentException(
            "Unrecognized or unsupported column data type: " + columnType);
      }
    }

    private static void validateManifest(TableManifest tableManifest, Ddl ddl) {
      Table table = ddl.table(tableManifest.getTableName());
      if (table == null) {
        throw new RuntimeException(
            String.format(
                "Table %s not found in the database. Table must be pre-created in database",
                tableManifest.getTableName()));
      }

      for (TableManifest.Column manifiestColumn : tableManifest.getColumnsList()) {
        Column dbColumn = table.column(manifiestColumn.getColumnName());
        if (dbColumn == null) {
          throw new RuntimeException(
              String.format(
                  "Column %s in manifest does not exist in DB table %s.",
                  manifiestColumn.getColumnName(), table.name()));
        }
        if (parseSpannerDataType(manifiestColumn.getTypeName()) != dbColumn.type().getCode()) {
          throw new RuntimeException(
              String.format(
                  "Mismatching type: Table %s Column %s [%s from DB and %s from manifest]",
                  table.name(), dbColumn.name(), dbColumn.type(), manifiestColumn.getTypeName()));
        }
      }
    }
  }

  @VisibleForTesting
  static class ReadTableColumns
      extends PTransform<
          PCollection<ImportManifest>, PCollection<Map<String, List<TableManifest.Column>>>> {

    @Override
    public PCollection<Map<String, List<TableManifest.Column>>> expand(
        PCollection<ImportManifest> input) {
      return input.apply(
          "Resolve manifest to table name and file name",
          ParDo.of(
              new DoFn<ImportManifest, Map<String, List<TableManifest.Column>>>() {

                @ProcessElement
                public void processElement(ProcessContext c) {
                  ImportManifest manifest = c.element();

                  Map<String, List<TableManifest.Column>> columnsMap =
                      new HashMap<String, List<TableManifest.Column>>();
                  for (ImportManifest.TableManifest table : manifest.getTablesList()) {
                    columnsMap.put(table.getTableName().toLowerCase(), table.getColumnsList());
                  }
                  c.output(columnsMap);
                }
              }));
    }
  }
}
