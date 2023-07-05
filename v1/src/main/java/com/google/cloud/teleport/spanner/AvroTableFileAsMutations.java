/*
 * Copyright (C) 2019 Google LLC
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

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.Table;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.AvroSource;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.options.ValueProvider;
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
 * <p>Input PCollection is a @{code KV&lt;filePath, tableName&gt;}
 */
class AvroTableFileAsMutations
    extends PTransform<PCollection<KV<String, String>>, PCollection<Mutation>> {

  // Schema of the Spanner database that the file is going to be imported into.
  private final PCollectionView<Ddl> ddlView;

  public AvroTableFileAsMutations(PCollectionView<Ddl> ddlView) {
    this.ddlView = ddlView;
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
        // PCollection<FileIO.ReadableFile>
        .apply(
            "Split into ranges",
            ParDo.of(
                    new SplitIntoRangesFn(
                        SplitIntoRangesFn.DEFAULT_BUNDLE_SIZE,
                        filenamesToTableNamesMapView,
                        ValueProvider.StaticValueProvider.of(false)))
                .withSideInputs(filenamesToTableNamesMapView))
        .setCoder(FileShard.Coder.of())
        // PCollection<FileShard>
        .apply("Reshuffle", Reshuffle.viaRandomKey())
        // PCollection<FileShard>

        .apply("Read ranges", ParDo.of(new ReadFileRangesFn(ddlView)).withSideInputs(ddlView));
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
