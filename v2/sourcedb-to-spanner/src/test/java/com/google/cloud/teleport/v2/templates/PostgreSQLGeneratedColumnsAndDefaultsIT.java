/*
 * Copyright (C) 2025 Google LLC
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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests a migration from a
 * PostgreSQL database whose Spanner schema, as produced by Spanner Migration Tool, contains
 * generated columns and column defaults. Both are omitted from the mutation as "auto-value" columns
 * so that Spanner populates them.
 *
 * <p>Each dialect is covered because Spanner Migration Tool does not produce the same schema for
 * both: the PostgreSQL dialect accepts "::" casts, so degraded_gencol.label stays generated there
 * while GoogleSQL degrades it to a plain column. The migrated values must match regardless.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class PostgreSQLGeneratedColumnsAndDefaultsIT extends SourceDbToSpannerITBase {

  private static boolean initialized = false;

  public static PostgresResourceManager postgresSQLResourceManager;
  public static SpannerResourceManager gsqlSpannerResourceManager;
  public static SpannerResourceManager pgDialectSpannerResourceManager;

  private static final String POSTGRESQL_DDL_RESOURCE =
      "GeneratedColumnsAndDefaultsIT/postgresql-schema.sql";
  private static final String SPANNER_GSQL_DDL_RESOURCE =
      "GeneratedColumnsAndDefaultsIT/spanner-gsql-schema.sql";
  private static final String SPANNER_PG_DDL_RESOURCE =
      "GeneratedColumnsAndDefaultsIT/spanner-pg-schema.sql";

  @Before
  public void setUp() throws Exception {
    synchronized (PostgreSQLGeneratedColumnsAndDefaultsIT.class) {
      if (!initialized) {
        postgresSQLResourceManager = setUpPostgreSQLResourceManager();
        gsqlSpannerResourceManager = setUpSpannerResourceManager();
        pgDialectSpannerResourceManager = setUpPGDialectSpannerResourceManager();

        loadSQLFileResource(postgresSQLResourceManager, POSTGRESQL_DDL_RESOURCE);

        initialized = true;
      }
    }
  }

  @AfterClass
  public static void cleanUp() {
    ResourceManagerUtils.cleanResources(
        postgresSQLResourceManager, gsqlSpannerResourceManager, pgDialectSpannerResourceManager);
  }

  @Test
  public void testPostgreSQLGeneratedColumnsAndDefaultsGoogleSQLDialect() throws Exception {
    createSpannerDDL(gsqlSpannerResourceManager, SPANNER_GSQL_DDL_RESOURCE);
    runPipeline(gsqlSpannerResourceManager, "GSQL");
    verifyData(gsqlSpannerResourceManager);
  }

  @Test
  public void testPostgreSQLGeneratedColumnsAndDefaultsPostgreSQLDialect() throws Exception {
    createSpannerDDL(pgDialectSpannerResourceManager, SPANNER_PG_DDL_RESOURCE);
    runPipeline(pgDialectSpannerResourceManager, "PG");
    verifyData(pgDialectSpannerResourceManager);
  }

  private void runPipeline(SpannerResourceManager spannerResourceManager, String jobSuffix)
      throws Exception {
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            getClass().getSimpleName() + jobSuffix,
            null,
            null,
            postgresSQLResourceManager,
            spannerResourceManager,
            jobParameters(),
            null);

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();
  }

  private Map<String, String> jobParameters() {
    Map<String, String> params = new HashMap<>();
    if (System.getProperty("directRunnerTest") != null) {
      params.put("resourceHints", "cpu_count=4");
    }
    return params;
  }

  private void verifyData(SpannerResourceManager spannerResourceManager) {
    assertGeneratedColumnsComputedBySpanner(spannerResourceManager);
    assertNonStoredGeneratedColumn(spannerResourceManager);
    assertGeneratedColumnWithNullInput(spannerResourceManager);
    assertGeneratedPrimaryKey(spannerResourceManager);
    assertDefaultsApplied(spannerResourceManager);
    assertSourceNullsBeatDefaults(spannerResourceManager);
    assertLabelMatchesSource(spannerResourceManager);
  }

  /** total and sku_upper are generated in Spanner, so the pipeline must not write them. */
  private void assertGeneratedColumnsComputedBySpanner(
      SpannerResourceManager spannerResourceManager) {
    ImmutableList<Struct> rows =
        spannerResourceManager.runQuery(
            "SELECT id, price, qty, total, sku, sku_upper FROM products ORDER BY id");
    assertThat(rows).hasSize(3);

    assertProduct(rows.get(0), 1L, 10L, 2L, 20L, "abc", "ABC");
    assertProduct(rows.get(1), 2L, 5L, 6L, 30L, "xyz", "XYZ");
    assertProduct(rows.get(2), 3L, 100L, 1L, 100L, "p-3", "P-3");
  }

  private void assertProduct(
      Struct row, long id, long price, long qty, long total, String sku, String skuUpper) {
    assertThat(row.getLong("id")).isEqualTo(id);
    assertThat(row.getLong("price")).isEqualTo(price);
    assertThat(row.getLong("qty")).isEqualTo(qty);
    assertThat(row.getLong("total")).isEqualTo(total);
    assertThat(row.getString("sku")).isEqualTo(sku);
    assertThat(row.getString("sku_upper")).isEqualTo(skuUpper);
  }

  /**
   * doubled is generated but not STORED. IS_GENERATED does not distinguish the two, so it is
   * skipped like any other generated column. The source column is STORED only because the
   * testcontainer is PostgreSQL 15 and VIRTUAL needs 18; the Spanner side drives the skip.
   */
  private void assertNonStoredGeneratedColumn(SpannerResourceManager spannerResourceManager) {
    ImmutableList<Struct> rows =
        spannerResourceManager.runQuery("SELECT id, a, tag, doubled FROM gc_virtual ORDER BY id");
    assertThat(rows).hasSize(2);

    assertThat(rows.get(0).getLong("a")).isEqualTo(4L);
    assertThat(rows.get(0).getString("tag")).isEqualTo("four");
    assertThat(rows.get(0).getLong("doubled")).isEqualTo(8L);

    assertThat(rows.get(1).getLong("a")).isEqualTo(25L);
    assertThat(rows.get(1).getString("tag")).isEqualTo("twentyfive");
    assertThat(rows.get(1).getLong("doubled")).isEqualTo(50L);
  }

  /** A null input makes the expression null, matching what PostgreSQL stored. */
  private void assertGeneratedColumnWithNullInput(SpannerResourceManager spannerResourceManager) {
    ImmutableList<Struct> rows =
        spannerResourceManager.runQuery("SELECT id, x, y, sum_xy FROM gc_nullable ORDER BY id");
    assertThat(rows).hasSize(2);

    assertThat(rows.get(0).getLong("sum_xy")).isEqualTo(7L);
    assertThat(rows.get(1).isNull("x")).isTrue();
    assertThat(rows.get(1).isNull("sum_xy")).isTrue();
  }

  /** k is a generated primary key: rows land under the right keys only if Spanner computes them. */
  private void assertGeneratedPrimaryKey(SpannerResourceManager spannerResourceManager) {
    ImmutableList<Struct> rows =
        spannerResourceManager.runQuery("SELECT k, tag, a FROM gc_pk ORDER BY k");
    assertThat(rows).hasSize(2);

    assertThat(rows.get(0).getLong("k")).isEqualTo(2L);
    assertThat(rows.get(0).getString("tag")).isEqualTo("x");
    assertThat(rows.get(0).getLong("a")).isEqualTo(1L);

    assertThat(rows.get(1).getLong("k")).isEqualTo(11L);
    assertThat(rows.get(1).getString("tag")).isEqualTo("y");
    assertThat(rows.get(1).getLong("a")).isEqualTo(10L);
  }

  /**
   * Columns present at source carry the source value. The d_spanner_* columns have no source
   * counterpart, so Spanner applies its own defaults; d_derived is generated from one of them.
   */
  private void assertDefaultsApplied(SpannerResourceManager spannerResourceManager) {
    ImmutableList<Struct> rows = defaultsRows(spannerResourceManager);
    assertThat(rows).hasSize(4);

    for (int i = 0; i < 2; i++) {
      Struct row = rows.get(i);
      assertThat(row.getLong("d_int")).isEqualTo(42L);
      assertThat(row.getLong("d_bigint")).isEqualTo(9000000000L);
      assertThat(row.getString("d_str")).isEqualTo("NEW");
      assertThat(row.getBoolean("d_bool")).isTrue();
      assertThat(row.getLong("d_neg")).isEqualTo(-7L);
    }

    Struct explicit = rows.get(2);
    assertThat(explicit.getString("payload")).isEqualTo("row-three");
    assertThat(explicit.getLong("d_int")).isEqualTo(1L);
    assertThat(explicit.getLong("d_bigint")).isEqualTo(2L);
    assertThat(explicit.getString("d_str")).isEqualTo("OLD");
    assertThat(explicit.getBoolean("d_bool")).isFalse();
    assertThat(explicit.getLong("d_neg")).isEqualTo(3L);

    for (Struct row : rows) {
      assertThat(row.getLong("d_spanner_only")).isEqualTo(777L);
      assertThat(row.getLong("d_spanner_notnull")).isEqualTo(555L);
      assertThat(row.getLong("d_derived")).isEqualTo(1110L);
    }
  }

  /** An explicit null at source must stay null rather than falling back to the Spanner DEFAULT. */
  private void assertSourceNullsBeatDefaults(SpannerResourceManager spannerResourceManager) {
    Struct nulls = defaultsRows(spannerResourceManager).get(3);
    assertThat(nulls.getString("payload")).isEqualTo("row-four");
    assertThat(nulls.isNull("d_int")).isTrue();
    assertThat(nulls.isNull("d_bigint")).isTrue();
    assertThat(nulls.isNull("d_str")).isTrue();
    assertThat(nulls.isNull("d_bool")).isTrue();
    assertThat(nulls.isNull("d_neg")).isTrue();
  }

  private ImmutableList<Struct> defaultsRows(SpannerResourceManager spannerResourceManager) {
    return spannerResourceManager.runQuery(
        "SELECT id, payload, d_int, d_bigint, d_str, d_bool, d_neg, d_spanner_only,"
            + " d_spanner_notnull, d_derived FROM defaults_all ORDER BY id");
  }

  /**
   * label is generated at source but plain in GoogleSQL Spanner, so it is copied verbatim there and
   * recomputed in the PostgreSQL dialect. The migrated value must match the source either way.
   */
  private void assertLabelMatchesSource(SpannerResourceManager spannerResourceManager) {
    ImmutableList<Struct> rows =
        spannerResourceManager.runQuery("SELECT id, a, b, label FROM degraded_gencol ORDER BY id");
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getString("label")).isEqualTo("2-3");
    assertThat(rows.get(1).getString("label")).isEqualTo("10-20");
  }
}
