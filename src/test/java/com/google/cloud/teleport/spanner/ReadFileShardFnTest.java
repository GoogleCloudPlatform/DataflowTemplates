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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for ReadFileShardFn class. */
public final class ReadFileShardFnTest {
  private static final Logger LOG = LoggerFactory.getLogger(ReadFileShardFnTest.class);

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static String testTableName = "TestTable";

  private final ValueProvider<Character> columnDelimiter = StaticValueProvider.of(',');
  private final ValueProvider<Character> fieldQualifier = StaticValueProvider.of('"');
  private final ValueProvider<Boolean> trailingDelimiter = StaticValueProvider.of(false);
  private final ValueProvider<Character> escapeChar = StaticValueProvider.of(null);
  private final ValueProvider<String> nullString = StaticValueProvider.of(null);
  private final ValueProvider<Boolean> handleNewLine = StaticValueProvider.of(true);

  @Test
  public void readFileBasicTest() throws Exception {
    Path inputFile = Files.createTempFile(testTableName, ".csv");

    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, charset)) {
      String data = "1,abc,def\n2,abc,def\n3,abc,def\n4,abc,def";
      writer.write(data, 0, data.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
    PCollection<FileShard> fileShard =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply(
                "Create file shard collection",
                ParDo.of(
                    new DoFn<FileIO.ReadableFile, FileShard>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        c.output(
                            FileShard.create(
                                testTableName, c.element(), new OffsetRange(0L, 30L), 3L));
                      }
                    }))
            .setCoder(FileShard.Coder.of());

    PCollection<KV<String, CSVRecord>> records =
        fileShard.apply(
            ParDo.of(
                new ReadFileShardFn(
                    columnDelimiter,
                    fieldQualifier,
                    trailingDelimiter,
                    escapeChar,
                    nullString,
                    handleNewLine)));
    // We convert the CSVRecord to a string containing the values as PAssert cannot compare equality
    // for CSVRecord directly.
    PCollection<KV<String, String>> csvValues =
        records.apply(
            "get values",
            ParDo.of(
                new DoFn<KV<String, CSVRecord>, KV<String, String>>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    CSVRecord csvRecord = c.element().getValue();
                    List<String> vals = new ArrayList<String>();
                    for (int i = 0; i < csvRecord.size(); i++) {
                      vals.add(csvRecord.get(i));
                    }
                    c.output(KV.of(c.element().getKey(), String.join(",", vals)));
                  }
                }));
    List<CSVRecord> csvRecords =
        CSVParser.parse(
                "1,abc,def\n2,abc,def\n3,abc,def\n", CSVFormat.newFormat(columnDelimiter.get()))
            .getRecords();

    List<KV<String, String>> expectedRecords =
        IntStream.range(0, 3)
            .mapToObj(i -> KV.of(testTableName, csvRecordToValues(csvRecords.get(i))))
            .collect(Collectors.toList());

    PAssert.that(csvValues).containsInAnyOrder(expectedRecords);

    pipeline.run();
  }

  @Test
  public void readMiddleShard() throws Exception {
    Path inputFile = Files.createTempFile(testTableName, ".csv");

    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, charset)) {
      String data = "1,abc,def\n2,abc,def\n3,abc,def\n4,abc,def";
      writer.write(data, 0, data.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
    PCollection<FileShard> fileShard =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply(
                "Create file shard collection",
                ParDo.of(
                    new DoFn<FileIO.ReadableFile, FileShard>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        c.output(
                            FileShard.create(
                                testTableName, c.element(), new OffsetRange(10L, 30L), 2L));
                      }
                    }))
            .setCoder(FileShard.Coder.of());

    PCollection<KV<String, CSVRecord>> records =
        fileShard.apply(
            ParDo.of(
                new ReadFileShardFn(
                    columnDelimiter,
                    fieldQualifier,
                    trailingDelimiter,
                    escapeChar,
                    nullString,
                    handleNewLine)));
    // We convert the CSVRecord to a string containing the values as PAssert cannot compare equality
    // for CSVRecord directly.
    PCollection<KV<String, String>> csvValues =
        records.apply(
            "get values",
            ParDo.of(
                new DoFn<KV<String, CSVRecord>, KV<String, String>>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    CSVRecord csvRecord = c.element().getValue();
                    List<String> vals = new ArrayList<String>();
                    for (int i = 0; i < csvRecord.size(); i++) {
                      vals.add(csvRecord.get(i));
                    }
                    c.output(KV.of(c.element().getKey(), String.join(",", vals)));
                  }
                }));
    List<CSVRecord> csvRecords =
        CSVParser.parse("2,abc,def\n3,abc,def\n", CSVFormat.newFormat(columnDelimiter.get()))
            .getRecords();

    List<KV<String, String>> expectedRecords =
        IntStream.range(0, 2)
            .mapToObj(i -> KV.of(testTableName, csvRecordToValues(csvRecords.get(i))))
            .collect(Collectors.toList());

    PAssert.that(csvValues).containsInAnyOrder(expectedRecords);

    pipeline.run();
  }

  @Test
  public void readLastShardWithoutNewline() throws Exception {
    Path inputFile = Files.createTempFile(testTableName, ".csv");

    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, charset)) {
      String data = "1,abc,def\n2,abc,def\n3,abc,def\n4,abc,def";
      writer.write(data, 0, data.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
    PCollection<FileShard> fileShard =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply(
                "Create file shard collection",
                ParDo.of(
                    new DoFn<FileIO.ReadableFile, FileShard>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        c.output(
                            FileShard.create(
                                testTableName, c.element(), new OffsetRange(30L, 39L), 1L));
                      }
                    }))
            .setCoder(FileShard.Coder.of());

    PCollection<KV<String, CSVRecord>> records =
        fileShard.apply(
            ParDo.of(
                new ReadFileShardFn(
                    columnDelimiter,
                    fieldQualifier,
                    trailingDelimiter,
                    escapeChar,
                    nullString,
                    handleNewLine)));
    // We convert the CSVRecord to a string containing the values as PAssert cannot compare equality
    // for CSVRecord directly.
    PCollection<KV<String, String>> csvValues =
        records.apply(
            "get values",
            ParDo.of(
                new DoFn<KV<String, CSVRecord>, KV<String, String>>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    CSVRecord csvRecord = c.element().getValue();
                    List<String> vals = new ArrayList<String>();
                    for (int i = 0; i < csvRecord.size(); i++) {
                      vals.add(csvRecord.get(i));
                    }
                    c.output(KV.of(c.element().getKey(), String.join(",", vals)));
                  }
                }));
    List<CSVRecord> csvRecords =
        CSVParser.parse("4,abc,def", CSVFormat.newFormat(columnDelimiter.get())).getRecords();

    List<KV<String, String>> expectedRecords =
        IntStream.range(0, 1)
            .mapToObj(i -> KV.of(testTableName, csvRecordToValues(csvRecords.get(i))))
            .collect(Collectors.toList());

    PAssert.that(csvValues).containsInAnyOrder(expectedRecords);

    pipeline.run();
  }

  @Test
  public void readLastShardWithNewline() throws Exception {
    Path inputFile = Files.createTempFile(testTableName, ".csv");

    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, charset)) {
      String data = "1,abc,def\n2,abc,def\n3,abc,def\n4,abc,def\n";
      writer.write(data, 0, data.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
    PCollection<FileShard> fileShard =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply(
                "Create file shard collection",
                ParDo.of(
                    new DoFn<FileIO.ReadableFile, FileShard>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        c.output(
                            FileShard.create(
                                testTableName, c.element(), new OffsetRange(30L, 40L), 1L));
                      }
                    }))
            .setCoder(FileShard.Coder.of());

    PCollection<KV<String, CSVRecord>> records =
        fileShard.apply(
            ParDo.of(
                new ReadFileShardFn(
                    columnDelimiter,
                    fieldQualifier,
                    trailingDelimiter,
                    escapeChar,
                    nullString,
                    handleNewLine)));
    // We convert the CSVRecord to a string containing the values as PAssert cannot compare equality
    // for CSVRecord directly.
    PCollection<KV<String, String>> csvValues =
        records.apply(
            "get values",
            ParDo.of(
                new DoFn<KV<String, CSVRecord>, KV<String, String>>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    CSVRecord csvRecord = c.element().getValue();
                    List<String> vals = new ArrayList<String>();
                    for (int i = 0; i < csvRecord.size(); i++) {
                      vals.add(csvRecord.get(i));
                    }
                    c.output(KV.of(c.element().getKey(), String.join(",", vals)));
                  }
                }));
    List<CSVRecord> csvRecords =
        CSVParser.parse("4,abc,def\n", CSVFormat.newFormat(columnDelimiter.get())).getRecords();

    List<KV<String, String>> expectedRecords =
        IntStream.range(0, 1)
            .mapToObj(i -> KV.of(testTableName, csvRecordToValues(csvRecords.get(i))))
            .collect(Collectors.toList());

    PAssert.that(csvValues).containsInAnyOrder(expectedRecords);

    pipeline.run();
  }

  public String csvRecordToValues(CSVRecord csvRecord) {
    List<String> vals = new ArrayList<String>();
    for (int i = 0; i < csvRecord.size(); i++) {
      vals.add(csvRecord.get(i));
    }
    return String.join(",", vals);
  }
}
