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
package com.google.cloud.teleport.v2.templates.sqlserver;

import static com.google.cloud.teleport.v2.templates.constants.Constants.SOURCE_SQLSERVER;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.MSSQLResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * An integration test for {@link SpannerToSourceDb} Flex template which verifies that reserved
 * keywords can be used as table and column names on the SQL Server target.
 *
 * <p>Two tables are exercised:
 *
 * <ul>
 *   <li>{@code [true]} with columns {@code COLUMN}, {@code TABLE}, {@code WITH} — the direct port
 *       of the MySQL reference test.
 *   <li>{@code [ORDER]} with primary key {@code KEY} and one column per remaining Transact-SQL
 *       reserved keyword in {@code sqlserver-reserved-keywords.txt}, so that every keyword in the
 *       official list appears as a table name, key column or value column. It is driven through
 *       INSERT, UPDATE and DELETE so each DML shape the template generates (MERGE insert/update and
 *       DELETE) runs with quoted keyword identifiers.
 * </ul>
 *
 * <p>No session file is used; schema mapping comes from discovery, so the resources here are
 * independent of the other sources' reserved-keyword tests.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSQLServerReservedKeywordsIT extends SpannerToSourceDbITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "sqlserver/SpannerToSQLServerReservedKeywordsIT/spanner-schema.sql";
  private static final String SQLSERVER_DDL_RESOURCE =
      "sqlserver/SpannerToSQLServerReservedKeywordsIT/sqlserver-schema.sql";

  /** T-SQL reserved keyword list; the schema resources are generated from it. */
  private static final String KEYWORDS_RESOURCE =
      "sqlserver/SpannerToSQLServerReservedKeywordsIT/sqlserver-reserved-keywords.txt";

  /** Table ported from the MySQL reference test. */
  private static final String BASIC_TABLE = "true";

  private static final String BASIC_PK = "COLUMN";
  private static final List<String> BASIC_COLS = List.of("TABLE", "WITH");

  /** Table covering every remaining reserved keyword (must match the generated schemas). */
  private static final String ALL_KEYWORDS_TABLE = "ORDER";

  private static final String ALL_KEYWORDS_PK = "KEY";

  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static MSSQLResourceManager mssqlResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
    spannerMetadataResourceManager = createSpannerMetadataDatabase();
    mssqlResourceManager = setUpMSSQLResourceManager(testName);
    createSQLServerSchema(mssqlResourceManager, SQLSERVER_DDL_RESOURCE);
    gcsResourceManager = setUpSpannerITGcsResourceManager();
    createAndUploadShardConfigToGcs(gcsResourceManager, mssqlResourceManager);
    pubsubResourceManager = setUpPubSubResourceManager();
    subscriptionName =
        createPubsubResources(
            getClass().getSimpleName(),
            pubsubResourceManager,
            getGcsPath("dlq", gcsResourceManager)
                .replace("gs://" + gcsResourceManager.getBucket(), ""),
            gcsResourceManager);
    // No session file: the template discovers the schema and maps identically-named tables and
    // columns, which is exactly what this test needs to verify for reserved keywords.
    jobInfo =
        launchDataflowJob(
            gcsResourceManager,
            spannerResourceManager,
            spannerMetadataResourceManager,
            subscriptionName.toString(),
            null,
            null,
            null,
            null,
            null,
            SOURCE_SQLSERVER,
            new HashMap<>());
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        mssqlResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testSpannerToSQLServerReservedKeywords() throws Exception {
    assertThatPipeline(jobInfo).isRunning();

    // Part 1: original scenario ported 1:1 from the MySQL reference test ([true] table).
    spannerResourceManager.write(generateBasicData());
    waitForRowCount(BASIC_TABLE, 2);

    List<Map<String, Object>> rows = mssqlResourceManager.readTable("[" + BASIC_TABLE + "]");
    assertThat(rows).hasSize(2);
    rows.sort(Comparator.comparing(r -> (Long) r.get(BASIC_PK)));

    assertThat(rows.get(0).get("COLUMN")).isEqualTo(1L);
    assertThat(rows.get(0).get("TABLE")).isEqualTo("table1");
    assertThat(rows.get(0).get("WITH")).isEqualTo("with1");

    assertThat(rows.get(1).get("COLUMN")).isEqualTo(2L);
    assertThat(rows.get(1).get("TABLE")).isEqualTo("table2");
    assertThat(rows.get(1).get("WITH")).isEqualTo("with2");

    // Part 2: every other T-SQL reserved keyword, as table name ([ORDER]), primary key ([KEY]) or
    // value column, driven through INSERT, UPDATE and DELETE.
    List<String> keywords = loadReservedKeywords();
    assertThat(keywords).containsAtLeast(ALL_KEYWORDS_TABLE, ALL_KEYWORDS_PK, BASIC_PK);
    assertThat(keywords).containsAtLeastElementsIn(BASIC_COLS);
    // Keywords already exercised by [true], plus this table's own name and key, are excluded.
    List<String> valueColumns =
        keywords.stream()
            .filter(k -> !k.equals(ALL_KEYWORDS_TABLE) && !k.equals(ALL_KEYWORDS_PK))
            .filter(k -> !k.equals(BASIC_PK) && !BASIC_COLS.contains(k))
            .toList();

    // INSERT: two rows, every reserved-keyword column populated.
    spannerResourceManager.write(
        List.of(buildRow(1L, valueColumns, "v1_"), buildRow(2L, valueColumns, "v2_")));
    waitForRowCount(ALL_KEYWORDS_TABLE, 2);
    Map<Long, Map<String, Object>> keywordRows = readAllKeywordRowsByPk();
    assertRow(keywordRows.get(1L), valueColumns, "v1_");
    assertRow(keywordRows.get(2L), valueColumns, "v2_");

    // UPDATE: rewrite every column of row 1; row 2 must be untouched.
    spannerResourceManager.write(List.of(buildRow(1L, valueColumns, "u1_")));
    waitFor(() -> "u1_ADD".equals(readAllKeywordRowsByPk().get(1L).get("ADD")));
    keywordRows = readAllKeywordRowsByPk();
    assertThat(keywordRows).hasSize(2);
    assertRow(keywordRows.get(1L), valueColumns, "u1_");
    assertRow(keywordRows.get(2L), valueColumns, "v2_");

    // DELETE: remove row 1; row 2 must survive.
    spannerResourceManager.write(List.of(Mutation.delete(ALL_KEYWORDS_TABLE, Key.of(1L))));
    waitForRowCount(ALL_KEYWORDS_TABLE, 1);
    keywordRows = readAllKeywordRowsByPk();
    assertThat(keywordRows.keySet()).containsExactly(2L);
    assertRow(keywordRows.get(2L), valueColumns, "v2_");
  }

  private static List<Mutation> generateBasicData() {
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newInsertOrUpdateBuilder(BASIC_TABLE)
            .set("COLUMN")
            .to(1)
            .set("TABLE")
            .to("table1")
            .set("WITH")
            .to("with1")
            .build());
    mutations.add(
        Mutation.newInsertOrUpdateBuilder(BASIC_TABLE)
            .set("COLUMN")
            .to(2)
            .set("TABLE")
            .to("table2")
            .set("WITH")
            .to("with2")
            .build());
    return mutations;
  }

  /** Reads the T-SQL reserved keyword list that the schema resources are generated from. */
  private static List<String> loadReservedKeywords() throws IOException {
    return Resources.readLines(Resources.getResource(KEYWORDS_RESOURCE), StandardCharsets.UTF_8)
        .stream()
        .map(String::trim)
        .filter(l -> !l.isEmpty() && !l.startsWith("#"))
        .toList();
  }

  /** Builds a row whose value in column {@code c} is {@code prefix + c}, e.g. {@code v1_SELECT}. */
  private static Mutation buildRow(long pk, List<String> valueColumns, String prefix) {
    Mutation.WriteBuilder builder =
        Mutation.newInsertOrUpdateBuilder(ALL_KEYWORDS_TABLE).set(ALL_KEYWORDS_PK).to(pk);
    for (String c : valueColumns) {
      builder.set(c).to(prefix + c);
    }
    return builder.build();
  }

  private static void assertRow(Map<String, Object> row, List<String> valueColumns, String prefix) {
    assertThat(row).isNotNull();
    for (String c : valueColumns) {
      assertWithMessage("column [%s]", c).that(row.get(c)).isEqualTo(prefix + c);
    }
  }

  private static Map<Long, Map<String, Object>> readAllKeywordRowsByPk() {
    Map<Long, Map<String, Object>> byPk = new HashMap<>();
    for (Map<String, Object> row : mssqlResourceManager.readTable("[" + ALL_KEYWORDS_TABLE + "]")) {
      byPk.put(((Number) row.get(ALL_KEYWORDS_PK)).longValue(), row);
    }
    return byPk;
  }

  private void waitForRowCount(String table, long expected) {
    waitFor(() -> mssqlResourceManager.getRowCount("[" + table + "]") == expected);
  }

  private void waitFor(Supplier<Boolean> condition) {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(10)), condition::get);
    assertThatResult(result).meetsConditions();
  }
}
