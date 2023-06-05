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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.spanner.IntegrationTest;
import com.google.cloud.teleport.v2.spanner.SpannerServerResource;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import com.google.cloud.teleport.v2.templates.session.Session;
import com.google.cloud.teleport.v2.templates.spanner.ProcessInformationSchema;
import com.google.cloud.teleport.v2.templates.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.templates.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.util.Arrays;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 * An integration test that writes change events to Cloud Spanner. This requires an active GCP
 * project with a Spanner instance and the test can only be run locally with a project set up using
 * 'gcloud config'. TODO: Add support for emulator to run tests without a Cloud Spanner instance.
 */
@Category(IntegrationTest.class)
public class SpannerStreamingWriteIntegrationTest {
  private final String testDb = "testdb";

  @Rule public final TemporaryFolder tmpDir = new TemporaryFolder();
  @Rule public final SpannerServerResource spannerServer = new SpannerServerResource();

  @Rule public final transient TestPipeline testPipeline = TestPipeline.create();

  @Before
  public void setup() throws Exception {
    // Just to make sure an old database is not left over.
    spannerServer.dropDatabase(testDb);
    createTablesForTest();
  }

  @After
  public void teardown() {
    spannerServer.dropDatabase(testDb);
  }

  private Ddl readDdl(String db) {
    DatabaseClient dbClient = spannerServer.getDbClient(db);
    Ddl ddl;
    try (ReadOnlyTransaction ctx = dbClient.readOnlyTransaction()) {
      ddl = new InformationSchemaScanner(ctx).scan();
    }
    return ddl;
  }

  private void createTablesForTest() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("Table1")
            .column("id")
            .int64()
            .endColumn()
            .column("data")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .createTable("Table1_interleaved")
            .column("id")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .column("data2")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .desc("id2")
            .end()
            .interleaveInParent("Table1")
            .endTable()
            .build();
    spannerServer.createDatabase(testDb, ddl.statements());
  }

  private void verifyRecordCountinTable(String tableName, long count) {
    try (ReadContext rc = spannerServer.getDbClient(testDb).singleUse();
        ResultSet rs = rc.executeQuery(Statement.of("Select count(*) from " + tableName)); ) {
      rs.next();
      Struct result = rs.getCurrentRowAsStruct();
      assertEquals(count, result.getLong(0));
    }
  }

  private void verifyDataInTable1(long primaryKey, long data) {
    try (ReadContext rc = spannerServer.getDbClient(testDb).singleUse();
        ResultSet rs =
            rc.executeQuery(Statement.of("Select data from Table1 where id = " + primaryKey)); ) {
      assertTrue(rs.next());
      Struct result = rs.getCurrentRowAsStruct();
      assertEquals(data, result.getLong(0));
    }
  }

  private void constructAndRunPipeline(PCollection<FailsafeElement<String, String>> jsonRecords) {
    String shadowTablePrefix = "shadow";
    SpannerConfig sourceConfig = spannerServer.getSpannerConfig(testDb);
    PCollection<Ddl> ddl =
        testPipeline.apply(
            "Process Information Schema",
            new ProcessInformationSchema(sourceConfig, true, shadowTablePrefix, "oracle"));
    PCollectionView<Ddl> ddlView = ddl.apply("Cloud Spanner DDL as view", View.asSingleton());
    Session session = new Session();

    jsonRecords.apply(
        "Write events to Cloud Spanner",
        new SpannerTransactionWriter(
            sourceConfig, ddlView, session, null, shadowTablePrefix, "oracle", false));

    PipelineResult testResult = testPipeline.run();
    testResult.waitUntilFinish();
  }

  private JSONObject getChangeEvent(String tableName, String changeType, String scn) {
    JSONObject json = new JSONObject();
    json.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, DatastreamConstants.ORACLE_SOURCE_TYPE);
    json.put(DatastreamConstants.EVENT_CHANGE_TYPE_KEY, changeType);
    json.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, tableName);
    json.put(DatastreamConstants.ORACLE_TIMESTAMP_KEY, 1);
    json.put(DatastreamConstants.ORACLE_SCN_KEY, scn);
    return json;
  }

  private JSONObject getChangeEventForTable1(
      String id, String data, String changeType, String scn) {
    JSONObject json = getChangeEvent("Table1", changeType, scn);
    json.put("id", id);
    json.put("data", data);
    return json;
  }

  @Test
  public void canWriteInsertChangeEvents() throws Exception {
    JSONObject json1 = getChangeEventForTable1("1", "334", "INSERT", "1");
    JSONObject json2 = getChangeEventForTable1("2", "32", "INSERT", "3");

    PCollection<FailsafeElement<String, String>> jsonRecords =
        testPipeline.apply(
            Create.of(
                    Arrays.asList(
                        FailsafeElement.of(json1.toString(), json1.toString()),
                        FailsafeElement.of(json2.toString(), json2.toString())))
                .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

    constructAndRunPipeline(jsonRecords);

    verifyRecordCountinTable("Table1", 2);
    verifyDataInTable1(1, 334);
    verifyDataInTable1(2, 32);
  }

  @Test
  public void canUpdateExistingRecord() throws Exception {
    JSONObject json1 = getChangeEventForTable1("1", "10", "INSERT", "1");
    JSONObject json2 = getChangeEventForTable1("1", "20", "UPDATE", "3");

    PCollection<FailsafeElement<String, String>> jsonRecords =
        testPipeline.apply(
            Create.of(
                    Arrays.asList(
                        FailsafeElement.of(json1.toString(), json1.toString()),
                        FailsafeElement.of(json2.toString(), json2.toString())))
                .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

    constructAndRunPipeline(jsonRecords);

    verifyRecordCountinTable("Table1", 1);
    verifyDataInTable1(1, 20);
  }

  // @Test
  public void canUpdateWithDisorderedAndDuplicatedEvents() throws Exception {
    JSONObject json1 = getChangeEventForTable1("1", "10", "INSERT", "1");
    JSONObject json2 = getChangeEventForTable1("1", "20", "UPDATE", "3");

    PCollection<FailsafeElement<String, String>> jsonRecords =
        testPipeline.apply(
            Create.of(
                    Arrays.asList(
                        FailsafeElement.of(json2.toString(), json2.toString()),
                        FailsafeElement.of(json1.toString(), json1.toString()),
                        FailsafeElement.of(json2.toString(), json2.toString()),
                        FailsafeElement.of(json1.toString(), json1.toString()),
                        FailsafeElement.of(json2.toString(), json2.toString()),
                        FailsafeElement.of(json2.toString(), json2.toString()),
                        FailsafeElement.of(json1.toString(), json1.toString())))
                .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

    constructAndRunPipeline(jsonRecords);

    verifyRecordCountinTable("Table1", 1);
    verifyDataInTable1(1, 20);
  }

  @Test
  public void canWriteDisorderedAndInterleavedChangeEvents() throws Exception {
    JSONObject json1 = getChangeEventForTable1("1", "334", "INSERT", "1");
    JSONObject json2 = getChangeEvent("Table1_interleaved", "INSERT", "2");
    json2.put("id", "1");
    json2.put("id2", "1");
    json2.put("data2", "32");

    /* The order of event processing cannot be predicted or controlled in
     * Test pipelines. The order in the Arrays below does not mean the events
     * are processed in that order.
     * As long as atleast 1 change event for the interleaved table is after the
     * parent table, this test will be successful.
     * Hence change event for interleaved table is repeated multiple times.
     * This also mimics the retry behavior during interleaved tables handling.
     */
    PCollection<FailsafeElement<String, String>> jsonRecords =
        testPipeline.apply(
            Create.of(
                    Arrays.asList(
                        FailsafeElement.of(json2.toString(), json2.toString()),
                        FailsafeElement.of(json2.toString(), json2.toString()),
                        FailsafeElement.of(json1.toString(), json1.toString()),
                        FailsafeElement.of(json2.toString(), json2.toString()),
                        FailsafeElement.of(json2.toString(), json2.toString()),
                        FailsafeElement.of(json2.toString(), json2.toString()),
                        FailsafeElement.of(json2.toString(), json2.toString()),
                        FailsafeElement.of(json2.toString(), json2.toString())))
                .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

    constructAndRunPipeline(jsonRecords);

    verifyRecordCountinTable("Table1", 1);
    verifyRecordCountinTable("Table1_interleaved", 1);
  }

  @Test
  public void canIgnoreCaseWhileEventProcessing() throws Exception {
    JSONObject json1 = getChangeEvent("Table1", "INSERT", "1");
    json1.put("ID", "1");
    json1.put("dAtA", "23");
    JSONObject json2 = getChangeEvent("Table1", "INSERT", "1");
    json2.put("iD", "2");
    json2.put("DaTa", "23");

    PCollection<FailsafeElement<String, String>> jsonRecords =
        testPipeline.apply(
            Create.of(
                    Arrays.asList(
                        FailsafeElement.of(json1.toString(), json1.toString()),
                        FailsafeElement.of(json2.toString(), json2.toString())))
                .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

    constructAndRunPipeline(jsonRecords);

    verifyRecordCountinTable("Table1", 2);
    verifyDataInTable1(1, 23);
    verifyDataInTable1(2, 23);
  }
}
