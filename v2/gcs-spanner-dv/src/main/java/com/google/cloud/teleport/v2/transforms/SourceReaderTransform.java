/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.coders.GenericRecordCoder;
import com.google.cloud.teleport.v2.config.TableConfiguration;
import com.google.cloud.teleport.v2.dofn.SourceHashFn;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.fn.IdentityGenericRecordFn;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.common.base.CharMatcher;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.jetbrains.annotations.NotNull;

/**
 * Reads the source Avro files under {@code gcsInputDirectory} and hashes their records.
 *
 * <p>Bulk migration output is laid out as root/table/shardId/file.avro. Which files get listed
 * depends on the configured filters:
 *
 * <ul>
 *   <li>No filters: one pattern, root/**.avro. Every file is read.
 *   <li>Tables only: one pattern per table, root/table/**.avro.
 *   <li>Tables and shards: one pattern per table and shard, root/table/shardId/**.avro.
 *   <li>Shards only: one pattern, root/**.avro, then {@link #isConfiguredFile} keeps the selected
 *       shards. A per-shard pattern with a wildcard table folder would list all of root once per
 *       shard.
 *   <li>A table name or shard ID containing * ? [ or \: same as shards only. Beam globs can't
 *       escape these characters, so such a name can't be written in a pattern.
 * </ul>
 */
public class SourceReaderTransform
    extends PTransform<@NotNull PBegin, @NotNull PCollection<ComparisonRecord>> {

  private final String gcsInputDirectory;
  private final PCollectionView<Ddl> ddlView;
  private final SerializableFunction<Ddl, ISchemaMapper> schemaMapperProvider;
  private final CustomTransformation customTransformation;
  private final TableConfiguration tableConfig;

  public SourceReaderTransform(
      String gcsInputDirectory,
      PCollectionView<Ddl> ddlView,
      SerializableFunction<Ddl, ISchemaMapper> schemaMapperProvider,
      CustomTransformation customTransformation,
      TableConfiguration tableConfig) {
    this.gcsInputDirectory = gcsInputDirectory;
    this.ddlView = ddlView;
    this.schemaMapperProvider = schemaMapperProvider;
    this.customTransformation = customTransformation;
    this.tableConfig = tableConfig;
  }

  @Override
  public @NotNull PCollection<ComparisonRecord> expand(PBegin input) {
    PCollection<MatchResult.Metadata> files =
        input
            .apply("CreateFilePatterns", Create.of(getFilePatterns(gcsInputDirectory, tableConfig)))
            .apply(
                "MatchFilePatterns",
                FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW));
    if (needsFileFilter(tableConfig)) {
      String root = stripTrailingSlash(gcsInputDirectory);
      TableConfiguration config = tableConfig;
      files =
          files.apply(
              "KeepConfiguredFiles",
              Filter.by(file -> isConfiguredFile(root, config, file.resourceId().toString())));
    }
    return files
        .apply(
            "ReadMatchedFiles",
            FileIO.readMatches()
                .withDirectoryTreatment(FileIO.ReadMatches.DirectoryTreatment.PROHIBIT))
        .apply(
            "ReadSourceAvroRecords",
            AvroIO.parseFilesGenericRecords(new IdentityGenericRecordFn())
                .withCoder(GenericRecordCoder.of()))
        .apply(
            "CalculateSourceRecordsHash",
            ParDo.of(new SourceHashFn(ddlView, schemaMapperProvider, customTransformation))
                .withSideInputs(ddlView));
  }

  static List<String> getFilePatterns(String gcsInputDirectory, TableConfiguration tableConfig) {
    String root = stripTrailingSlash(gcsInputDirectory);
    // Without a table filter, or with a name Beam globs can't express, list root once.
    // A shard-only filter root/*/shard pattern would re-list all of root for every shard, so this
    // is handled by the filtering flow as well.
    if (tableConfig == null || !tableConfig.hasTableFilters() || hasGlobChar(tableConfig)) {
      return List.of(root + "/**.avro");
    }
    List<String> patterns = new ArrayList<>();
    for (String table : tableConfig.getSourceTables()) {
      if (!tableConfig.hasShardFilter()) {
        patterns.add(root + "/" + table + "/**.avro");
      } else {
        for (String shardId : tableConfig.getShardIds()) {
          // The '/' after the shard ID keeps shard_1 from matching shard_10.
          patterns.add(root + "/" + table + "/" + shardId + "/**.avro");
        }
      }
    }
    return patterns;
  }

  /**
   * True iff {@link #getFilePatterns} lists all of root while tables or shards are selected, so the
   * matched files must be filtered with {@link #isConfiguredFile}: shards-only runs, and names
   * containing glob characters.
   */
  private static boolean needsFileFilter(TableConfiguration tableConfig) {
    return tableConfig != null
        && ((tableConfig.hasShardFilter() && !tableConfig.hasTableFilters())
            || hasGlobChar(tableConfig));
  }

  /** True iff a table name or shard ID contains a character Beam globs can't escape. */
  private static boolean hasGlobChar(TableConfiguration tableConfig) {
    return Stream.concat(tableConfig.getSourceTables().stream(), tableConfig.getShardIds().stream())
        .anyMatch(CharMatcher.anyOf("*?[\\")::matchesAnyOf);
  }

  /**
   * True iff {@code path} (root/table/shard/file.avro) is under a configured table and, when shards
   * are selected, a configured shard.
   */
  static boolean isConfiguredFile(String root, TableConfiguration tableConfig, String path) {
    String[] segments = path.substring(root.length() + 1).split("/", 3);
    if (tableConfig.hasTableFilters() && !tableConfig.getSourceTables().contains(segments[0])) {
      return false;
    }
    return !tableConfig.hasShardFilter()
        || (segments.length == 3 && tableConfig.getShardIds().contains(segments[1]));
  }

  private static String stripTrailingSlash(String path) {
    return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }
}
