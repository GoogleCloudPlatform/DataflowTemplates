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
package com.google.cloud.teleport.v2.templates.spanner;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.teleport.v2.spanner.IntegrationTest;
import com.google.cloud.teleport.v2.spanner.SpannerServerResource;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/** An integration test that validates shadow table creation. */
@Category(IntegrationTest.class)
public class ProcessInformationSchemaIntegrationTest {
  private final String testDb = "testdb";

  @Rule public final TemporaryFolder tmpDir = new TemporaryFolder();
  @Rule public final SpannerServerResource spannerServer = new SpannerServerResource();

  @Rule public final transient TestPipeline testPipeline = TestPipeline.create();

  @Before
  public void setup() {
    // Just to make sure an old database is not left over.
    spannerServer.dropDatabase(testDb);
  }

  @After
  public void teardown() {
    spannerServer.dropDatabase(testDb);
  }

  private void createDb(Ddl ddl) throws Exception {
    spannerServer.createDatabase(testDb, ddl.statements());
  }

  private Ddl readDdl(String db) {
    DatabaseClient dbClient = spannerServer.getDbClient(db);
    Ddl ddl;
    try (ReadOnlyTransaction ctx = dbClient.readOnlyTransaction()) {
      ddl = new InformationSchemaScanner(ctx).scan();
    }
    return ddl;
  }

  private Ddl.Builder getTestDdlBuilder() {
    return Ddl.builder()
        .createTable("Table")
        .column("ID")
        .int64()
        .endColumn()
        .column("data")
        .int64()
        .endColumn()
        .primaryKey()
        .asc("ID")
        .end()
        .endTable()
        .createTable("Table_interleaved")
        .column("ID")
        .int64()
        .endColumn()
        .column("ID2")
        .int64()
        .endColumn()
        .column("data")
        .int64()
        .endColumn()
        .primaryKey()
        .asc("ID")
        .asc("ID2")
        .end()
        .interleaveInParent("Table")
        .endTable();
  }

  @Test
  public void canCreateShadowTablesForAllDataTables() throws Exception {
    SpannerConfig sourceConfig = spannerServer.getSpannerConfig(testDb);
    Ddl testDdl = getTestDdlBuilder().build();
    createDb(testDdl);

    testPipeline.apply(
        "Process Information Schema",
        new ProcessInformationSchema(
            sourceConfig, /* shouldCreateShadowTables= */ true, "shadow", "oracle"));
    PipelineResult testResult = testPipeline.run();
    testResult.waitUntilFinish();
    Ddl finalDdl = readDdl(testDb);

    Table shadowTable = finalDdl.table("shadow_Table");
    Table shadowTableInterleaved = finalDdl.table("shadow_Table_interleaved");
    assertNotNull(shadowTable);
    assertNotNull(shadowTableInterleaved);
    assertEquals(4, finalDdl.allTables().size());

    assertThat(shadowTable.primaryKeys(), is(testDdl.table("Table").primaryKeys()));
    assertEquals(shadowTable.columns().size(), testDdl.table("Table").primaryKeys().size() + 2);

    assertThat(
        shadowTableInterleaved.primaryKeys(), is(testDdl.table("Table_interleaved").primaryKeys()));
    assertEquals(
        shadowTableInterleaved.columns().size(),
        testDdl.table("Table_interleaved").primaryKeys().size() + 2);
  }

  @Test
  public void canFlagProtectShadowTableCreation() throws Exception {
    SpannerConfig sourceConfig = spannerServer.getSpannerConfig(testDb);
    Ddl testDdl = getTestDdlBuilder().build();
    createDb(testDdl);

    testPipeline.apply(
        "Read Information Schema",
        new ProcessInformationSchema(
            sourceConfig, /* shouldCreateShadowTables= */ false, "shadow", "oracle"));
    PipelineResult testResult = testPipeline.run();
    testResult.waitUntilFinish();
    Ddl finalDdl = readDdl(testDb);

    Table shadowTable = finalDdl.table("shadow_Table");
    Table shadowTableInterleaved = finalDdl.table("shadow_Table_interleaved");
    assertNull(shadowTable);
    assertNull(shadowTableInterleaved);
    assertEquals(2, finalDdl.allTables().size());
  }

  @Test
  public void canCreateMissingShadowTables() throws Exception {
    SpannerConfig sourceConfig = spannerServer.getSpannerConfig(testDb);
    Ddl testDdl =
        getTestDdlBuilder()
            .createTable("shadow_Table")
            .column("ID")
            .int64()
            .endColumn()
            .column("version")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("ID")
            .end()
            .endTable()
            .build();
    createDb(testDdl);

    testPipeline.apply(
        "Process Information Schema",
        new ProcessInformationSchema(
            sourceConfig, /* shouldCreateShadowTables= */ true, "shadow", "oracle"));
    PipelineResult testResult = testPipeline.run();
    testResult.waitUntilFinish();
    Ddl finalDdl = readDdl(testDb);

    assertEquals(4, finalDdl.allTables().size());
    Table shadowTable = finalDdl.table("shadow_Table");
    Table shadowTableInterleaved = finalDdl.table("shadow_Table_interleaved");
    assertNotNull(shadowTable);
    assertNotNull(shadowTableInterleaved);

    assertThat(
        shadowTableInterleaved.primaryKeys(), is(testDdl.table("Table_interleaved").primaryKeys()));
    assertEquals(
        shadowTableInterleaved.columns().size(),
        testDdl.table("Table_interleaved").primaryKeys().size() + 2);
  }
}
