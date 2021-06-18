/*
 * Copyright (C) 2021 Google Inc.
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

package com.google.cloud.teleport.templates;

import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.spanner.IntegrationTest;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.templates.SpannerToText.CreateTransactionFnWithTimestamp;
import com.google.cloud.teleport.templates.common.SpannerConverters;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.LocalSpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An end to end test that exports a database at different times and verfies the behavior of export
 * with timestamp and without timestamp. This requires an active GCP project with a Spanner
 * instance. Hence this test can only be run locally with a project set up using 'gcloud config'.
 */
@Category(IntegrationTest.class)
public final class SpannerToTextTest {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToTextTest.class);

  static String tmpDir = Files.createTempDir().getAbsolutePath();

  private final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
  private final long numericTime = timestamp.getTime();
  private final String sourceDb = "test-db-" + Long.toString(numericTime);
  private final String destDbPrefix = "import";
  private final String tableName = "Users";
  private final String chkpt1 = "chkpt1";
  private final String chkpt2 = "chkpt2";

  // Multiple pipelines needed as everything is run in a single test
  @Rule public final transient TestPipeline exportPipeline1 = TestPipeline.create();
  @Rule public final transient TestPipeline exportPipeline2 = TestPipeline.create();

  @Rule
  public final SpannerServerResource spannerServer = new SpannerServerResource();

  @After
  public void teardown() {
    spannerServer.dropDatabase(sourceDb);
    spannerServer.dropDatabase(destDbPrefix + chkpt1);
    spannerServer.dropDatabase(destDbPrefix + chkpt2);
  }

  /* Creates a database for a given Spanner database and populates it with
   * with random data */
  private void createAndPopulate(String db, Ddl ddl, int numBatches) throws Exception {
    spannerServer.createDatabase(db, ddl.statements());
    spannerServer.populateRandomData(db, ddl, numBatches);
  }

  String getCurrentTimestamp() {
    Instant instant = Instant.now();
    return instant.toString();
  }

  /* Validates behavior of database export without specifying timestamp
   * and with timestamp specified */
  @Test
  public void runExportWithTsTest() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable(tableName)
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("age")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .end()
            .endTable()
            .build();

    /* Create initial table and populate
     * numBatches = 100
     */
    createAndPopulate(sourceDb, ddl, 100);

    // Export the database and note the timestamp ts1
    spannerServer.createDatabase(destDbPrefix + chkpt1, Collections.emptyList());
    exportDbAtTime(sourceDb, destDbPrefix + chkpt1, chkpt1, "", exportPipeline1);

    // Save the timestamp directly after the export
    String chkPt1Ts = getCurrentTimestamp();

    // Sleep for some time before adding more to the table
    Thread.sleep(10000);

    // Add more records to the table, export the database and note the timestamp ts3
    spannerServer.populateRandomData(sourceDb, ddl, 100);
    spannerServer.createDatabase(destDbPrefix + chkpt2, Collections.emptyList());

    // Export the table from the database using the saved timestamp
    exportDbAtTime(sourceDb, destDbPrefix + chkpt2, chkpt2, chkPt1Ts, exportPipeline2);

    File folder = new File(tmpDir + "/");

    // Store the contents of directory containing the exported CSVs into a List
    File[] files = folder.listFiles();

    List<String> oldData = readDbData(files, chkpt1);
    List<String> expectedOldData = readDbData(files, chkpt2);

    // Sort statements
    Collections.sort(oldData);
    Collections.sort(expectedOldData);

    assertEquals(oldData, expectedOldData);
  }

  private void exportDbAtTime(
      String sourceDb, String destDb, String jobIdName, String ts, TestPipeline exportPipeline) {

    ValueProvider.StaticValueProvider<String> destination =
        ValueProvider.StaticValueProvider.of(tmpDir + "/");
    ValueProvider.StaticValueProvider<String> jobId =
        ValueProvider.StaticValueProvider.of(jobIdName);
    ValueProvider.StaticValueProvider<String> source =
        ValueProvider.StaticValueProvider.of(tmpDir + "/" + jobIdName);
    ValueProvider.StaticValueProvider<String> table =
        ValueProvider.StaticValueProvider.of(tableName);
    ValueProvider.StaticValueProvider<String> timestamp = ValueProvider.StaticValueProvider.of(ts);
    ValueProvider.StaticValueProvider<Boolean> exportAsLogicalType =
        ValueProvider.StaticValueProvider.of(false);
    SpannerConfig sourceConfig = spannerServer.getSpannerConfig(sourceDb);

    PCollectionView<Transaction> tx =
        exportPipeline
            .apply("Setup for Transaction", Create.of(1))
            .apply(
                "Create transaction",
                ParDo.of(new CreateTransactionFnWithTimestamp(sourceConfig, timestamp)))
            .apply("As PCollectionView", View.asSingleton());

    // Export Pipeline code; taken from SpannerToText.java
    PTransform<PBegin, PCollection<ReadOperation>> spannerExport =
        SpannerConverters.ExportTransformFactory.create(table, sourceConfig, destination);

    PCollection<String> csv =
        exportPipeline
            .apply("Create export", spannerExport)
            .apply(
                "Read all records",
                LocalSpannerIO.readAll().withTransaction(tx).withSpannerConfig(sourceConfig))
            .apply(
                "Struct To Csv",
                MapElements.into(TypeDescriptors.strings())
                    .via(struct -> (new SpannerConverters.StructCsvPrinter()).print(struct)));
    csv.apply("Write to storage", TextIO.write().to(source).withSuffix(".csv"));
    exportPipeline.run();
  }

  /* Reads from a group of Files from a directory and stores the files
   * into memory given a String that their file names start with, startString */
  private List<String> readDbData(File[] files, String startString) throws Exception {
    List<String> data = Lists.newArrayList();

    for (File file : files) {
      if ((file.getName()).startsWith(startString)) {
        try {
          FileReader fileReader = new FileReader(file);

          // Convert fileReader to bufferedReader
          BufferedReader buffReader = new BufferedReader(fileReader);

          while (buffReader.ready()) {
            data.add(buffReader.readLine());
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return data;
  }
}
