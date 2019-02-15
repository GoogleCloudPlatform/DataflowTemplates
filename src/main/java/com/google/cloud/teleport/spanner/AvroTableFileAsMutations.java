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

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.Table;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.AvroSource;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.ReadableFileCoder;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Performs a sharded read of Avro export files and converts to {@link Mutation} objects.
 *
 * <p>This class is based on {@link org.apache.beam.sdk.io.ReadAllViaFileBasedSource} which is used
 * by {@link org.apache.beam.sdk.io.AvroIO.ReadAll}. A custom class is used because the {@link Ddl}
 * view is needed to get the table schema for the {@link AvroRecordConverter}. This is then used to
 * verify the table's schema against the Avro record schema.
 *
 * <p>Input PCollection is a @{code KV<filePath, tableName>}
 */
class AvroTableFileAsMutations
    extends PTransform<PCollection<KV<String, String>>, PCollection<Mutation>> {

  // Schema of the Spanner database that the file is going to be imported into.
  private final PCollectionView<Ddl> ddlView;

  public AvroTableFileAsMutations(PCollectionView<Ddl> ddlView) {
    this.ddlView = ddlView;
  }

  /** Helper class storing File, Table name and range to read. */
  @VisibleForTesting
  @AutoValue
  abstract static class FileShard {

    abstract String getTableName();

    abstract ReadableFile getFile();

    abstract OffsetRange getRange();

    static FileShard create(String tableName, ReadableFile file, OffsetRange range) {
      Preconditions.checkNotNull(tableName);
      Preconditions.checkNotNull(file);
      Preconditions.checkNotNull(range);
      return new AutoValue_AvroTableFileAsMutations_FileShard(tableName, file, range);
    }

    @Override
    public String toString() {
      return String.format(
          "FileShard(table:%s,file:%s:%s)",
          getTableName(), getFile().getMetadata().resourceId(), getRange().toString());
    }

    /**
     * Encodes/decodes a {@link FileShard}.
     *
     * <p>A custom Coder for this object is required because the {@link ReadableFile} member is not
     * serializable and requires a custom coder.
     */
    static class Coder extends AtomicCoder<FileShard> {
      private static final Coder INSTANCE = new Coder();

      public static Coder of() {
        return INSTANCE;
      }

      @Override
      public void encode(FileShard value, OutputStream os) throws IOException {
        StringUtf8Coder.of().encode(value.getTableName(), os);
        ReadableFileCoder.of().encode(value.getFile(), os);
        VarLongCoder.of().encode(value.getRange().getFrom(), os);
        VarLongCoder.of().encode(value.getRange().getTo(), os);
      }

      @Override
      public FileShard decode(InputStream is) throws IOException {
        String tableName = StringUtf8Coder.of().decode(is);
        ReadableFile file = ReadableFileCoder.of().decode(is);
        long from = VarLongCoder.of().decode(is);
        long to = VarLongCoder.of().decode(is);
        return new AutoValue_AvroTableFileAsMutations_FileShard(
            tableName, file, new OffsetRange(from, to));
      }
    }
  }

  @Override
  public PCollection<Mutation> expand(PCollection<KV<String, String>> filesToTables) {

    // Map<filename,tablename>
    PCollectionView<Map<String, String>> filenamesToTableNamesMapView =
        filesToTables.apply("asView", View.asMap());

    return filesToTables
        .apply("Get Filenames", Keys.create())
        // PCollection<String>
        .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
        // PCollection<Match.Metadata>
        .apply(FileIO.readMatches())
        // Pcollection<FileIO.ReadableFile>
        .apply(
            "Split into ranges",
            ParDo.of(
                    new SplitIntoRangesFn(
                        SplitIntoRangesFn.DEFAULT_BUNDLE_SIZE, filenamesToTableNamesMapView))
                .withSideInputs(filenamesToTableNamesMapView))
        .setCoder(FileShard.Coder.of())
        // PCollection<FileShard>
        .apply("Reshuffle", Reshuffle.viaRandomKey())
        // PCollection<FileShard>

        .apply("Read ranges", ParDo.of(new ReadFileRangesFn(ddlView)).withSideInputs(ddlView));
  }

  /**
   * Splits a table File into a set of {@link FileShard} objects storing the file, tablename and the
   * range offset/size.
   *
   * <p>Based on <code>SplitIntoRangesFn</code> in {@link
   * org.apache.beam.sdk.io.ReadAllViaFileBasedSource}.
   */
  @VisibleForTesting
  static class SplitIntoRangesFn extends DoFn<ReadableFile, FileShard> {
    static final long DEFAULT_BUNDLE_SIZE = 64 * 1024 * 1024L;

    final PCollectionView<Map<String, String>> filenamesToTableNamesMapView;
    private final long desiredBundleSize;

    SplitIntoRangesFn(
        long desiredBundleSize, PCollectionView<Map<String, String>> filenamesToTableNamesMapView) {
      this.filenamesToTableNamesMapView = filenamesToTableNamesMapView;
      this.desiredBundleSize = desiredBundleSize;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws FileNotFoundException {
      Map<String, String> filenamesToTableNamesMap = c.sideInput(filenamesToTableNamesMapView);
      Metadata metadata = c.element().getMetadata();
      String filename = metadata.resourceId().toString();
      String tableName = filenamesToTableNamesMap.get(filename);

      if (tableName == null) {
        throw new FileNotFoundException(
            "Unknown table name for file:" + filename + " in map " + filenamesToTableNamesMap);
      }

      if (!metadata.isReadSeekEfficient()) {
        // Do not shard the file.
        c.output(
            FileShard.create(tableName, c.element(), new OffsetRange(0, metadata.sizeBytes())));
      } else {
        // Create shards.
        for (OffsetRange range :
            new OffsetRange(0, metadata.sizeBytes()).split(desiredBundleSize, 0)) {
          c.output(FileShard.create(tableName, c.element(), range));
        }
      }
    }
  }

  /**
   * Given a {@link FileShard}, reads from the offset, and outputs {@link Mutation} objects for each
   * record.
   *
   * <p>Based on <code>ReadFileRangesFn</code> in {@link
   * org.apache.beam.sdk.io.ReadAllViaFileBasedSource}.
   */
  @VisibleForTesting
  static class ReadFileRangesFn extends DoFn<FileShard, Mutation> {

    private final PCollectionView<Ddl> ddlView;

    ReadFileRangesFn(PCollectionView<Ddl> ddlView) {
      this.ddlView = ddlView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      FileShard f = c.element();

      Ddl ddl = c.sideInput(ddlView);
      Table table = ddl.table(f.getTableName());
      SerializableFunction<GenericRecord, Mutation> parseFn = new AvroRecordConverter(table);
      AvroSource<Mutation> source =
          AvroSource.from(f.getFile().getMetadata().resourceId().toString())
              .withParseFn(parseFn, SerializableCoder.of(Mutation.class));
      try {
        BoundedSource.BoundedReader<Mutation> reader =
            source
                .createForSubrangeOfFile(
                    f.getFile().getMetadata(), f.getRange().getFrom(), f.getRange().getTo())
                .createReader(c.getPipelineOptions());
        for (boolean more = reader.start(); more; more = reader.advance()) {
          c.output(reader.getCurrent());
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
