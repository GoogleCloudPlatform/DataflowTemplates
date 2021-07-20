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

import static org.hamcrest.text.IsEqualCompressingWhiteSpace.equalToCompressingWhiteSpace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.InformationSchemaScanner;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * An end to end test that exports and imports a database at different times and verfies the
 * behavior of export with timestamp and without timestamp. This requires an active GCP project with
 * a Spanner instance. Hence this test can only be run locally with a project set up using 'gcloud
 * config'.
 */
@Category(IntegrationTest.class)
public class ExportTimestampTest {

  static String tmpDir = Files.createTempDir().getAbsolutePath();

  private final String sourceDb = "export";
  private final String destDbPrefix = "import";
  private final String chkpt1 = "chkpt1";
  private final String chkpt2 = "chkpt2";
  private final String chkpt3 = "chkpt3";
  private final String chkPt1WithTs = "cp1withts";
  private final String chkPt2WithTs = "cp2withts";
  private final String chkPt3WithTs = "cp3withts";

  // Multiple pipelines needed as everything is run in a single test
  @Rule public final transient TestPipeline exportPipeline1 = TestPipeline.create();
  @Rule public final transient TestPipeline importPipeline1 = TestPipeline.create();
  @Rule public final transient TestPipeline exportPipeline2 = TestPipeline.create();
  @Rule public final transient TestPipeline importPipeline2 = TestPipeline.create();
  @Rule public final transient TestPipeline exportPipeline3 = TestPipeline.create();
  @Rule public final transient TestPipeline importPipeline3 = TestPipeline.create();
  @Rule public final transient TestPipeline exportPipeline4 = TestPipeline.create();
  @Rule public final transient TestPipeline importPipeline4 = TestPipeline.create();
  @Rule public final transient TestPipeline exportPipeline5 = TestPipeline.create();
  @Rule public final transient TestPipeline importPipeline5 = TestPipeline.create();
  @Rule public final transient TestPipeline comparePipeline1 = TestPipeline.create();
  @Rule public final transient TestPipeline comparePipeline2 = TestPipeline.create();
  @Rule public final transient TestPipeline comparePipeline3 = TestPipeline.create();

  @Rule public final SpannerServerResource spannerServer = new SpannerServerResource();

  @Before
  public void setup() {
    spannerServer.dropDatabase(sourceDb);
    spannerServer.dropDatabase(destDbPrefix + chkpt1);
    spannerServer.dropDatabase(destDbPrefix + chkpt2);
    spannerServer.dropDatabase(destDbPrefix + chkpt3);
    spannerServer.dropDatabase(destDbPrefix + chkPt1WithTs);
    spannerServer.dropDatabase(destDbPrefix + chkPt2WithTs);
    spannerServer.dropDatabase(destDbPrefix + chkPt3WithTs);
  }

  @After
  public void teardown() {
    spannerServer.dropDatabase(sourceDb);
    spannerServer.dropDatabase(destDbPrefix + chkpt1);
    spannerServer.dropDatabase(destDbPrefix + chkpt2);
    spannerServer.dropDatabase(destDbPrefix + chkpt3);
    spannerServer.dropDatabase(destDbPrefix + chkPt1WithTs);
    spannerServer.dropDatabase(destDbPrefix + chkPt2WithTs);
    spannerServer.dropDatabase(destDbPrefix + chkPt3WithTs);
  }

  private void createAndPopulate(String db, Ddl ddl, int numBatches) throws Exception {
    try {
      ddl.prettyPrint(System.out);
    } catch (IOException e) {
      e.printStackTrace();
    }

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
            .createTable("Users")
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
            .createTable("AllTYPES")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("bool_field")
            .bool()
            .endColumn()
            .column("int64_field")
            .int64()
            .endColumn()
            .column("float64_field")
            .float64()
            .endColumn()
            .column("string_field")
            .string()
            .max()
            .endColumn()
            .column("bytes_field")
            .bytes()
            .max()
            .endColumn()
            .column("timestamp_field")
            .timestamp()
            .endColumn()
            .column("date_field")
            .date()
            .endColumn()
            .column("arr_bool_field")
            .type(Type.array(Type.bool()))
            .endColumn()
            .column("arr_int64_field")
            .type(Type.array(Type.int64()))
            .endColumn()
            .column("arr_float64_field")
            .type(Type.array(Type.float64()))
            .endColumn()
            .column("arr_string_field")
            .type(Type.array(Type.string()))
            .max()
            .endColumn()
            .column("arr_bytes_field")
            .type(Type.array(Type.bytes()))
            .max()
            .endColumn()
            .column("arr_timestamp_field")
            .type(Type.array(Type.timestamp()))
            .endColumn()
            .column("arr_date_field")
            .type(Type.array(Type.date()))
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .asc("id")
            .end()
            .interleaveInParent("Users")
            .onDeleteCascade()
            .endTable()
            .build();

    // Create initial table and populate
    createAndPopulate(sourceDb, ddl, 100);

    // Export the database and note the timestamp ts1
    spannerServer.createDatabase(destDbPrefix + chkpt1, Collections.emptyList());
    exportAndImportDbAtTime(
        sourceDb, destDbPrefix + chkpt1, chkpt1, "", exportPipeline1, importPipeline1);
    String chkPt1Ts = getCurrentTimestamp();

    Thread.sleep(2000);

    // Sleep for a couple of seconds and note the timestamp ts2
    String chkPt2Ts = getCurrentTimestamp();

    Thread.sleep(2000);

    // Add more records to the table, export the database and note the timestamp ts3
    spannerServer.populateRandomData(sourceDb, ddl, 100);
    spannerServer.createDatabase(destDbPrefix + chkpt3, Collections.emptyList());
    exportAndImportDbAtTime(
        sourceDb, destDbPrefix + chkpt3, chkpt3, "", exportPipeline2, importPipeline2);
    String chkPt3Ts = getCurrentTimestamp();

    // Export timestamp with timestamp ts1
    spannerServer.createDatabase(destDbPrefix + chkPt1WithTs, Collections.emptyList());
    exportAndImportDbAtTime(
        sourceDb, destDbPrefix + chkPt1WithTs,
        chkPt1WithTs, chkPt1Ts,
        exportPipeline3, importPipeline3);

    // Export timestamp with timestamp ts2
    spannerServer.createDatabase(destDbPrefix + chkPt2WithTs, Collections.emptyList());
    exportAndImportDbAtTime(
        sourceDb, destDbPrefix + chkPt2WithTs,
        chkPt2WithTs, chkPt2Ts,
        exportPipeline4, importPipeline4);

    // Export timestamp with timestamp ts3
    spannerServer.createDatabase(destDbPrefix + chkPt3WithTs, Collections.emptyList());
    exportAndImportDbAtTime(
        sourceDb,
        destDbPrefix + chkPt3WithTs,
        chkPt3WithTs,
        chkPt3Ts,
        exportPipeline5,
        importPipeline5);

    // Compare databases exported at ts1 and exported later specifying timestamp ts1
    compareDbs(destDbPrefix + chkpt1, destDbPrefix + chkPt1WithTs, comparePipeline1);
    // Compare databases exported at ts1 and exported later specifying timestamp ts2
    compareDbs(destDbPrefix + chkpt1, destDbPrefix + chkPt2WithTs, comparePipeline2);
    // Compare databases exported at ts3 and exported later specifying timestamp ts3
    compareDbs(destDbPrefix + chkpt3, destDbPrefix + chkPt3WithTs, comparePipeline3);
  }

  private void exportAndImportDbAtTime(
      String sourceDb,
      String destDb,
      String jobIdName,
      String ts,
      TestPipeline exportPipeline,
      TestPipeline importPipeline) {
    ValueProvider.StaticValueProvider<String> destination =
        ValueProvider.StaticValueProvider.of(tmpDir);
    ValueProvider.StaticValueProvider<String> jobId =
        ValueProvider.StaticValueProvider.of(jobIdName);
    ValueProvider.StaticValueProvider<String> source =
        ValueProvider.StaticValueProvider.of(tmpDir + "/" + jobIdName);
    ValueProvider.StaticValueProvider<String> timestamp = ValueProvider.StaticValueProvider.of(ts);
    ValueProvider.StaticValueProvider<Boolean> exportAsLogicalType =
        ValueProvider.StaticValueProvider.of(false);
    SpannerConfig sourceConfig = spannerServer.getSpannerConfig(sourceDb);
    exportPipeline.apply(
        "Export",
        new ExportTransform(sourceConfig, destination, jobId, timestamp, exportAsLogicalType));
    PipelineResult exportResult = exportPipeline.run();
    exportResult.waitUntilFinish();

    SpannerConfig copyConfig = spannerServer.getSpannerConfig(destDb);
    importPipeline.apply(
        "Import",
        new ImportTransform(
            copyConfig,
            source,
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true)));
    PipelineResult importResult = importPipeline.run();
    importResult.waitUntilFinish();
  }

  private void compareDbs(String sourceDb, String destDb, TestPipeline comparePipeline) {
    SpannerConfig sourceConfig = spannerServer.getSpannerConfig(sourceDb);
    SpannerConfig copyConfig = spannerServer.getSpannerConfig(destDb);
    PCollection<Long> mismatchCount =
        comparePipeline.apply("Compare", new CompareDatabases(sourceConfig, copyConfig));
    PAssert.that(mismatchCount)
        .satisfies(
            (x) -> {
              assertEquals(Lists.newArrayList(x), Lists.newArrayList(0L));
              return null;
            });
    PipelineResult compareResult = comparePipeline.run();
    compareResult.waitUntilFinish();

    Ddl sourceDdl = readDdl(sourceDb);
    Ddl destinationDdl = readDdl(destDb);

    assertThat(sourceDdl.prettyPrint(), equalToCompressingWhiteSpace(destinationDdl.prettyPrint()));
  }

  private Ddl readDdl(String db) {
    SpannerOptions spannerOptions = SpannerOptions.newBuilder().build();
    Spanner client = spannerOptions.getService();
    Ddl ddl;
    try (ReadOnlyTransaction ctx = spannerServer.getDbClient(db).readOnlyTransaction()) {
      ddl = new InformationSchemaScanner(ctx).scan();
    }
    return ddl;
  }
}
