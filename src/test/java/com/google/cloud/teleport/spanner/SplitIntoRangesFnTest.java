/*
 * Copyright (C) 2022 Google LLC
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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;

/** Tests for SplitIntoRangesFn class. */
public final class SplitIntoRangesFnTest implements Serializable {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static String testTableName = "TestTable";

  private final ValueProvider<Character> columnDelimiter = StaticValueProvider.of(',');
  private final ValueProvider<Character> fieldQualifier = StaticValueProvider.of('"');
  private final ValueProvider<Boolean> trailingDelimiter = StaticValueProvider.of(false);
  private final ValueProvider<Character> escapeChar = StaticValueProvider.of('\\');
  private final ValueProvider<Boolean> handleNewLine = StaticValueProvider.of(true);

  @Test
  public void handleNewLineSingleSplitTest() throws Exception {
    Path inputFile = Files.createTempFile(testTableName, ".csv");

    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, charset)) {
      String data = "1,abc,def\n" + "2,abc,def\n" + "3,abc,def\n" + "4,abc,def";
      writer.write(data, 0, data.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
    PCollectionView<Map<String, String>> filesToTablesMapView =
        pipeline
            .apply("filesToTablesMapView", Create.of(KV.of(inputFile.toString(), testTableName)))
            .apply(View.asMap());

    PCollection<FileShard> fileShards =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply(
                "Split into ranges",
                ParDo.of(
                        new SplitIntoRangesFn(
                            SplitIntoRangesFn.DEFAULT_BUNDLE_SIZE,
                            filesToTablesMapView,
                            fieldQualifier,
                            columnDelimiter,
                            escapeChar,
                            handleNewLine))
                    .withSideInputs(filesToTablesMapView))
            .setCoder(FileShard.Coder.of());
    PCollection<OffsetRange> offsetRanges =
        fileShards.apply(
            "Get offset ranges",
            ParDo.of(
                new DoFn<FileShard, OffsetRange>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().getRange());
                  }
                }));

    PCollection<Long> recordCounts =
        fileShards.apply(
            "Get record counts",
            ParDo.of(
                new DoFn<FileShard, Long>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().getRecordCount());
                  }
                }));

    PAssert.that(offsetRanges).containsInAnyOrder(new OffsetRange(0L, 39L));
    PAssert.that(recordCounts).containsInAnyOrder(4L);

    pipeline.run();
  }

  @Test
  public void handleNewLineTwoSplitTest() throws Exception {
    Path inputFile = Files.createTempFile(testTableName, ".csv");

    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, charset)) {
      String data = "1,abc,def\n" + "2,abc,def\n" + "3,abc,def\n" + "4,abc,def";
      writer.write(data, 0, data.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
    PCollectionView<Map<String, String>> filesToTablesMapView =
        pipeline
            .apply("filesToTablesMapView", Create.of(KV.of(inputFile.toString(), testTableName)))
            .apply(View.asMap());

    PCollection<FileShard> fileShards =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply(
                "Split into ranges",
                ParDo.of(
                        new SplitIntoRangesFn(
                            25L,
                            filesToTablesMapView,
                            fieldQualifier,
                            columnDelimiter,
                            escapeChar,
                            handleNewLine))
                    .withSideInputs(filesToTablesMapView))
            .setCoder(FileShard.Coder.of());
    PCollection<OffsetRange> offsetRanges =
        fileShards.apply(
            "Get offset ranges",
            ParDo.of(
                new DoFn<FileShard, OffsetRange>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().getRange());
                  }
                }));

    PCollection<Long> recordCounts =
        fileShards.apply(
            "Get record counts",
            ParDo.of(
                new DoFn<FileShard, Long>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().getRecordCount());
                  }
                }));

    PAssert.that(offsetRanges)
        .containsInAnyOrder(new OffsetRange(0L, 30L), new OffsetRange(30L, 39L));
    PAssert.that(recordCounts).containsInAnyOrder(3L, 1L);

    pipeline.run();
  }

  @Test
  public void handleNewLineSplitAtEveryLineTest() throws Exception {
    Path inputFile = Files.createTempFile(testTableName, ".csv");

    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, charset)) {
      String data = "1,abc,def\n" + "2,abc,def\n" + "3,abc,def\n" + "4,abc,def";
      writer.write(data, 0, data.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
    PCollectionView<Map<String, String>> filesToTablesMapView =
        pipeline
            .apply("filesToTablesMapView", Create.of(KV.of(inputFile.toString(), testTableName)))
            .apply(View.asMap());

    PCollection<FileShard> fileShards =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply(
                "Split into ranges",
                ParDo.of(
                        new SplitIntoRangesFn(
                            1L,
                            filesToTablesMapView,
                            fieldQualifier,
                            columnDelimiter,
                            escapeChar,
                            handleNewLine))
                    .withSideInputs(filesToTablesMapView))
            .setCoder(FileShard.Coder.of());
    PCollection<OffsetRange> offsetRanges =
        fileShards.apply(
            "Get offset ranges",
            ParDo.of(
                new DoFn<FileShard, OffsetRange>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().getRange());
                  }
                }));

    PCollection<Long> recordCounts =
        fileShards.apply(
            "Get record counts",
            ParDo.of(
                new DoFn<FileShard, Long>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().getRecordCount());
                  }
                }));

    PAssert.that(offsetRanges)
        .containsInAnyOrder(
            new OffsetRange(0L, 10L),
            new OffsetRange(10L, 20L),
            new OffsetRange(20L, 30L),
            new OffsetRange(30L, 39L));
    PAssert.that(recordCounts).containsInAnyOrder(1L, 1L, 1L, 1L);

    pipeline.run();
  }

  @Test
  public void customCharsAndDelimiters() throws Exception {
    Path inputFile = Files.createTempFile(testTableName, ".csv");

    Charset charset = Charset.forName("UTF-8");
    ValueProvider<Character> columnDelimiter = StaticValueProvider.of(':');
    ValueProvider<Character> fieldQualifier = StaticValueProvider.of('^');
    ValueProvider<Character> escapeChar = StaticValueProvider.of('#');
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, charset)) {
      String data = "1:^ab\nc^:def\n" + "2:abc:d##e#:f\n" + "3:abc:def\n" + "4:abc:def";
      writer.write(data, 0, data.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
    PCollectionView<Map<String, String>> filesToTablesMapView =
        pipeline
            .apply("filesToTablesMapView", Create.of(KV.of(inputFile.toString(), testTableName)))
            .apply(View.asMap());

    PCollection<FileShard> fileShards =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply(
                "Split into ranges",
                ParDo.of(
                        new SplitIntoRangesFn(
                            1L,
                            filesToTablesMapView,
                            fieldQualifier,
                            columnDelimiter,
                            escapeChar,
                            handleNewLine))
                    .withSideInputs(filesToTablesMapView))
            .setCoder(FileShard.Coder.of());
    PCollection<OffsetRange> offsetRanges =
        fileShards.apply(
            "Get offset ranges",
            ParDo.of(
                new DoFn<FileShard, OffsetRange>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().getRange());
                  }
                }));

    PCollection<Long> recordCounts =
        fileShards.apply(
            "Get record counts",
            ParDo.of(
                new DoFn<FileShard, Long>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().getRecordCount());
                  }
                }));

    PAssert.that(offsetRanges)
        .containsInAnyOrder(
            new OffsetRange(0L, 13L),
            new OffsetRange(13L, 27L),
            new OffsetRange(27L, 37L),
            new OffsetRange(37L, 46L));
    PAssert.that(recordCounts).containsInAnyOrder(1L, 1L, 1L, 1L);

    pipeline.run();
  }

  @Test
  public void unquoted() throws Exception {
    Path inputFile = Files.createTempFile(testTableName, ".csv");

    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, charset)) {
      String data = "1#,#\n,abc\n" + "2,abc##\n";
      writer.write(data, 0, data.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
    PCollectionView<Map<String, String>> filesToTablesMapView =
        pipeline
            .apply("filesToTablesMapView", Create.of(KV.of(inputFile.toString(), testTableName)))
            .apply(View.asMap());

    PCollection<FileShard> fileShards =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply(
                "Split into ranges",
                ParDo.of(
                        new SplitIntoRangesFn(
                            1L,
                            filesToTablesMapView,
                            fieldQualifier,
                            columnDelimiter,
                            StaticValueProvider.of('#'),
                            handleNewLine))
                    .withSideInputs(filesToTablesMapView))
            .setCoder(FileShard.Coder.of());
    PCollection<OffsetRange> offsetRanges =
        fileShards.apply(
            "Get offset ranges",
            ParDo.of(
                new DoFn<FileShard, OffsetRange>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().getRange());
                  }
                }));

    PCollection<Long> recordCounts =
        fileShards.apply(
            "Get record counts",
            ParDo.of(
                new DoFn<FileShard, Long>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().getRecordCount());
                  }
                }));

    PAssert.that(offsetRanges)
        .containsInAnyOrder(new OffsetRange(0L, 10L), new OffsetRange(10L, 18L));
    PAssert.that(recordCounts).containsInAnyOrder(1L, 1L);

    pipeline.run();
  }

  @Test
  public void quoted1() throws Exception {
    Path inputFile = Files.createTempFile(testTableName, ".csv");

    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, charset)) {
      String data = "1, \"abc\n" + "2,\"a\nbc\"\n" + "3,#\"abc";
      writer.write(data, 0, data.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
    PCollectionView<Map<String, String>> filesToTablesMapView =
        pipeline
            .apply("filesToTablesMapView", Create.of(KV.of(inputFile.toString(), testTableName)))
            .apply(View.asMap());

    PCollection<FileShard> fileShards =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply(
                "Split into ranges",
                ParDo.of(
                        new SplitIntoRangesFn(
                            1L,
                            filesToTablesMapView,
                            fieldQualifier,
                            columnDelimiter,
                            StaticValueProvider.of('#'),
                            handleNewLine))
                    .withSideInputs(filesToTablesMapView))
            .setCoder(FileShard.Coder.of());
    PCollection<OffsetRange> offsetRanges =
        fileShards.apply(
            "Get offset ranges",
            ParDo.of(
                new DoFn<FileShard, OffsetRange>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().getRange());
                  }
                }));

    PCollection<Long> recordCounts =
        fileShards.apply(
            "Get record counts",
            ParDo.of(
                new DoFn<FileShard, Long>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().getRecordCount());
                  }
                }));

    PAssert.that(offsetRanges)
        .containsInAnyOrder(
            new OffsetRange(0L, 8L), new OffsetRange(8L, 17L), new OffsetRange(17L, 24L));
    PAssert.that(recordCounts).containsInAnyOrder(1L, 1L, 1L);

    pipeline.run();
  }

  @Test
  public void quoted2() throws Exception {
    Path inputFile = Files.createTempFile(testTableName, ".csv");

    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, charset)) {
      String data =
          "1,\"a, b#\"c\"   ,\"def\"\n"
              + "2,\"a\nbc\"\n"
              + "3,\"ab\"\"c\"\n"
              + "4,#\"ab\n"
              + "5,\"xyz\"";
      writer.write(data, 0, data.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
    PCollectionView<Map<String, String>> filesToTablesMapView =
        pipeline
            .apply("filesToTablesMapView", Create.of(KV.of(inputFile.toString(), testTableName)))
            .apply(View.asMap());

    PCollection<FileShard> fileShards =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply(
                "Split into ranges",
                ParDo.of(
                        new SplitIntoRangesFn(
                            1L,
                            filesToTablesMapView,
                            fieldQualifier,
                            columnDelimiter,
                            StaticValueProvider.of('#'),
                            handleNewLine))
                    .withSideInputs(filesToTablesMapView))
            .setCoder(FileShard.Coder.of());
    PCollection<OffsetRange> offsetRanges =
        fileShards.apply(
            "Get offset ranges",
            ParDo.of(
                new DoFn<FileShard, OffsetRange>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().getRange());
                  }
                }));

    PCollection<Long> recordCounts =
        fileShards.apply(
            "Get record counts",
            ParDo.of(
                new DoFn<FileShard, Long>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().getRecordCount());
                  }
                }));

    PAssert.that(offsetRanges)
        .containsInAnyOrder(
            new OffsetRange(0L, 21L),
            new OffsetRange(21L, 30L),
            new OffsetRange(30L, 40L),
            new OffsetRange(40L, 47L),
            new OffsetRange(47L, 54L));
    PAssert.that(recordCounts).containsInAnyOrder(1L, 1L, 1L, 1L, 1L);

    pipeline.run();
  }

  @Test
  public void emptyRecordTest() throws Exception {
    Path inputFile = Files.createTempFile(testTableName, ".csv");

    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, charset)) {
      String data = "1,a,b\n\n2,x,y,z";
      writer.write(data, 0, data.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
    PCollectionView<Map<String, String>> filesToTablesMapView =
        pipeline
            .apply("filesToTablesMapView", Create.of(KV.of(inputFile.toString(), testTableName)))
            .apply(View.asMap());

    PCollection<FileShard> fileShards =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply(
                "Split into ranges",
                ParDo.of(
                        new SplitIntoRangesFn(
                            6L,
                            filesToTablesMapView,
                            fieldQualifier,
                            columnDelimiter,
                            escapeChar,
                            handleNewLine))
                    .withSideInputs(filesToTablesMapView))
            .setCoder(FileShard.Coder.of());
    PCollection<OffsetRange> offsetRanges =
        fileShards.apply(
            "Get offset ranges",
            ParDo.of(
                new DoFn<FileShard, OffsetRange>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().getRange());
                  }
                }));

    PCollection<Long> recordCounts =
        fileShards.apply(
            "Get record counts",
            ParDo.of(
                new DoFn<FileShard, Long>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().getRecordCount());
                  }
                }));

    PAssert.that(offsetRanges)
        .containsInAnyOrder(new OffsetRange(0L, 7L), new OffsetRange(7L, 14L));
    PAssert.that(recordCounts).containsInAnyOrder(2L, 1L);

    pipeline.run();
  }

  @Test(expected = PipelineExecutionException.class)
  public void dataAfterClosedQuote() throws Exception {
    Path inputFile = Files.createTempFile(testTableName, ".csv");

    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, charset)) {
      String data = "1,\"ab\"c";
      writer.write(data, 0, data.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
    PCollectionView<Map<String, String>> filesToTablesMapView =
        pipeline
            .apply("filesToTablesMapView", Create.of(KV.of(inputFile.toString(), testTableName)))
            .apply(View.asMap());

    PCollection<FileShard> fileShards =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply(
                "Split into ranges",
                ParDo.of(
                        new SplitIntoRangesFn(
                            1L,
                            filesToTablesMapView,
                            fieldQualifier,
                            columnDelimiter,
                            StaticValueProvider.of('#'),
                            handleNewLine))
                    .withSideInputs(filesToTablesMapView))
            .setCoder(FileShard.Coder.of());

    pipeline.run();
  }

  @Test
  public void legacyBasicSplitTest() throws Exception {
    Path inputFile = Files.createTempFile(testTableName, ".csv");

    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, charset)) {
      String data = "1,abc,def\n" + "2,abc,def\n" + "3,abc,def\n" + "4,abc,def";
      writer.write(data, 0, data.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
    PCollectionView<Map<String, String>> filesToTablesMapView =
        pipeline
            .apply("filesToTablesMapView", Create.of(KV.of(inputFile.toString(), testTableName)))
            .apply(View.asMap());

    PCollection<FileShard> fileShards =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply(
                "Split into ranges",
                ParDo.of(
                        new SplitIntoRangesFn(
                            20L,
                            filesToTablesMapView,
                            fieldQualifier,
                            columnDelimiter,
                            escapeChar,
                            StaticValueProvider.of(false)))
                    .withSideInputs(filesToTablesMapView))
            .setCoder(FileShard.Coder.of());
    PCollection<OffsetRange> offsetRanges =
        fileShards.apply(
            "Get offset ranges",
            ParDo.of(
                new DoFn<FileShard, OffsetRange>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().getRange());
                  }
                }));

    PCollection<Long> recordCounts =
        fileShards.apply(
            "Get record counts",
            ParDo.of(
                new DoFn<FileShard, Long>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().getRecordCount());
                  }
                }));

    PAssert.that(offsetRanges)
        .containsInAnyOrder(new OffsetRange(0L, 20L), new OffsetRange(20L, 39L));
    PAssert.that(recordCounts).containsInAnyOrder(Long.MAX_VALUE, Long.MAX_VALUE);

    pipeline.run();
  }
}
