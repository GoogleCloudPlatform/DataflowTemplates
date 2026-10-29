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
package com.google.cloud.teleport.v2.templates.oracle;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.ORACLE_SOURCE_TYPE;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.oracleutils.SpannerOracleResourceManager;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
import com.google.cloud.teleport.v2.templates.oracle.OracleDataTypesTestData.TypeTable;
import com.google.cloud.teleport.v2.templates.oracle.OracleDataTypesTestData.UnsupportedTable;
import com.google.pubsub.v1.SubscriptionName;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exhaustive data type integration test for the {@link SpannerToSourceDb} (reverse replication)
 * template from a <b>PostgreSQL-dialect</b> Spanner database to an Oracle target (scenario {@code
 * reverse-datatypes-pg}), driven by the {@code Spanner PostgreSQL} columns of the Oracle data type
 * mapping matrix.
 *
 * <p>For every Oracle type and alias in the matrix, rows are written to Spanner with the default
 * Spanner PostgreSQL type (scenario A), every alternative Spanner PostgreSQL type (scenario B) and
 * as a primary key (scenario C), and the replicated Oracle values are asserted. Types that
 * Datastream marks as unsupported and that the reverse generator can write are checked for
 * row-count consistency (scenario D). Tables, Spanner values, expected Oracle values and shared
 * execution/validation helpers are defined in {@link OracleDataTypesTestData}.
 *
 * <p>Matrix gaps resolved with the user: the blank PostgreSQL default for RAW, LONG RAW, BLOB and
 * the binary-like unsupported types is {@code bytea} (also used as the RAW primary key type); the
 * character types use {@code varchar(length)} and {@code bytea} as alternatives; Oracle NUMBER edge
 * cases {@code 1e38} and 38 nines are tested in {@code numeric} columns, which PostgreSQL-dialect
 * Spanner can hold.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToOracleDataTypesPGDialectIT extends SpannerToSourceDbITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToOracleDataTypesPGDialectIT.class);

  private static final String RESOURCE_DIR = "oracle/SpannerToOracleDataTypesPGDialectIT/";
  private static final String SPANNER_DDL_RESOURCE =
      RESOURCE_DIR + "oracle-POSTGRESQL-spanner-schema.sql";
  private static final String ORACLE_DDL_RESOURCE = RESOURCE_DIR + "oracle-schema.sql";

  /** Time to wait for all rows to be replicated to Oracle. */
  private static final int DATA_WAIT_MINUTES = 15;

  /**
   * Tables whose mapping is known not to behave as the matrix expects, with the root cause. They
   * are skipped when writing to Spanner, waiting, and validating so that unsupported or broken
   * mappings do not cause continuous dead-letter queue retry loops during pipeline execution.
   */
  private static final Map<String, String> KNOWN_MISMATCHES = knownMismatches();

  private static final Set<String> KNOWN_MISMATCH_TABLES = KNOWN_MISMATCHES.keySet();

  private static Map<String, String> knownMismatches() {
    Map<String, String> m = new HashMap<>();
    // BYTEA -> character columns: OracleDMLGenerator writes HEXTORAW(...) into the column, so
    // VARCHAR2/CHAR/CLOB store the hex text and N-types decode the UTF-8 bytes as UTF-16. Open bug.
    String bytesToText = "BUG: BYTEA written to a character column are not decoded as UTF-8 text";
    for (String t :
        List.of(
            "varchar2",
            "varchar",
            "char",
            "character",
            "nvarchar2",
            "nchar",
            "nchar_varying",
            "nat_character",
            "nat_char",
            "nat_characvar",
            "nat_charvar",
            "clob",
            "nclob")) {
      m.put(t + "_to_bytea_table", bytesToText);
    }
    // VARCHAR -> RAW/BLOB (matrix "VARCHAR (Base64)"): the generator writes the string unquoted.
    String stringToBinary =
        "Generator has no VARCHAR -> RAW/BLOB conversion (value written unquoted)";
    m.put("raw_to_varchar_table", stringToBinary);
    m.put("blob_to_varchar_table", stringToBinary);
    // BIGINT -> DATE/TIMESTAMP: no epoch conversion exists; the value is parsed as timestamp text.
    String int64ToTemporal =
        "Generator has no BIGINT -> DATE/TIMESTAMP conversion (rows go to DLQ)";
    for (String t : List.of("date", "timestamp", "timestamp_tz", "timestamp_ltz")) {
      m.put(t + "_to_bigint_table", int64ToTemporal);
    }
    // Scenario D: the generator cannot write these Oracle types (values written as raw literals).
    // LONG RAW is not listed: BYTEA values are written as HEXTORAW(...), which Oracle accepts.
    String unsupported = "Generator cannot write this Oracle type (rows go to DLQ)";
    for (String t :
        List.of(
            "interval_year_to_month",
            "interval_day_to_second",
            "bfile",
            "long",
            "rowid",
            "urowid",
            "xmltype",
            "anydata",
            "anytype",
            "anydataset",
            "uritype",
            "dburitype",
            "xdburitype",
            "httpuritype",
            "mlslabel",
            "ref",
            "varray",
            "nested_table",
            "object_type")) {
      m.put(t + "_table", unsupported);
    }
    return Map.copyOf(m);
  }

  private static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static SpannerOracleResourceManager jdbcResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static String oracleUsername;

  /**
   * Launches the pipeline. This class has a single {@code @Test}, so setup runs once; before adding
   * another test, guard the launch with {@code jobInfo == null} (as the other Oracle reverse ITs
   * do) so the job and the isolated Oracle user are not created again.
   */
  @Before
  public void setUp() throws Exception {
    // Assign the resource manager before running DDL so cleanUp() drops the database even if the
    // DDL fails.
    spannerResourceManager = setUpPGDialectSpannerResourceManager();
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    spannerMetadataResourceManager = createPGDialectSpannerMetadataDatabase();

    jdbcResourceManager = SharedOracleReverseITContainer.getInstance();
    oracleUsername = SharedOracleReverseITContainer.createIsolatedUser();
    testUsername = oracleUsername;
    SharedOracleReverseITContainer.createSchema(testUsername, ORACLE_DDL_RESOURCE);

    gcsResourceManager = setUpSpannerITGcsResourceManager();
    createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManager);
    pubsubResourceManager = setUpPubSubResourceManager();
    SubscriptionName subscriptionName =
        createPubsubResources(
            getClass().getSimpleName(),
            pubsubResourceManager,
            getGcsPath("dlq", gcsResourceManager)
                .replace("gs://" + gcsResourceManager.getBucket(), ""),
            gcsResourceManager);
    // No session file: Spanner and Oracle table/column names match exactly (identity mapping).
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
            ORACLE_SOURCE_TYPE,
            new HashMap<>(),
            Dialect.POSTGRESQL);
  }

  @AfterClass
  public static void cleanUp() {
    try {
      // jdbcResourceManager is the shared Oracle connection and must not be cleaned up here.
      ResourceManagerUtils.cleanResources(
          spannerResourceManager,
          spannerMetadataResourceManager,
          gcsResourceManager,
          pubsubResourceManager);
    } finally {
      SharedOracleReverseITContainer.dropIsolatedUser(oracleUsername);
      oracleUsername = null;
    }
  }

  /**
   * KNOWN LIMITATIONS &amp; MATRIX DISCREPANCIES (PostgreSQL-dialect Spanner -&gt; Oracle, reverse
   * replication)
   *
   * <p>The tables below are listed in {@link #KNOWN_MISMATCHES} and skipped when writing to
   * Spanner, waiting, and validating so they do not trigger continuous dead-letter queue retry
   * loops:
   *
   * <ul>
   *   <li><b>Spanner bytea -&gt; Oracle character columns (13 tables, e.g. {@code
   *       varchar2_to_bytea_table}, {@code nclob_to_bytea_table}) &mdash; open bug</b>: <code>
   *       OracleDMLGenerator</code> writes Spanner <code>bytea</code> as <code>HEXTORAW('...')
   *       </code>. For VARCHAR2/CHAR/CLOB, Oracle implicitly converts the RAW to its <i>hex
   *       text</i> (e.g. bytes of {@code "Hello"} become {@code "48656C6C6F"}). For
   *       NVARCHAR2/NCHAR/NCLOB it is wrapped in <code>UTL_RAW.CAST_TO_NVARCHAR2</code>, which
   *       interprets the UTF-8 bytes as the national character set (AL16UTF16), producing garbled
   *       text.
   *   <li><b>Spanner varchar -&gt; Oracle RAW/BLOB (2 tables: {@code raw_to_varchar_table}, {@code
   *       blob_to_varchar_table})</b>: the matrix lists VARCHAR (Base64) as an alternative, but the
   *       generator has no string-to-binary conversion and writes the value unquoted, so Oracle
   *       rejects the statement and the rows go to the dead-letter queue.
   *   <li><b>Spanner bigint -&gt; Oracle DATE/TIMESTAMP (4 tables: {@code date}, {@code timestamp},
   *       {@code timestamp_tz}, {@code timestamp_ltz} {@code _to_bigint_table})</b>: there is no
   *       epoch-to-temporal conversion; the integer is treated as timestamp text, which Oracle
   *       rejects, so the rows go to the dead-letter queue.
   *   <li><b>Types the generator cannot write (19 scenario D tables, e.g. INTERVAL, BFILE, LONG,
   *       ROWID/UROWID, XMLTYPE, ANYDATA, URI types, REF, VARRAY, nested tables, object types)</b>:
   *       values are written as plain literals that Oracle cannot convert, so the rows go to the
   *       dead-letter queue. LONG RAW is <i>not</i> affected because <code>bytea</code> values are
   *       written as <code>HEXTORAW(...)</code>, which Oracle accepts.
   * </ul>
   *
   * <p>Other behaviours that differ from the matrix but are asserted as expected:
   *
   * <ul>
   *   <li><b>Empty bytea -&gt; Oracle RAW/BLOB is NULL</b>: Oracle treats a zero-length RAW (and
   *       {@code ''}) as <code>NULL</code>, so empty Spanner <code>bytea</code> values are stored
   *       as <code>NULL</code>, consistent with how Oracle stores empty strings.
   * </ul>
   */
  @Test
  public void spannerToOracleDataTypesPGDialect() throws Exception {
    assertThatPipeline(jobInfo).isRunning();
    List<TypeTable> tables = OracleDataTypesTestData.postgresTables();
    List<UnsupportedTable> unsupportedTables = OracleDataTypesTestData.postgresUnsupportedTables();

    OracleDataTypesTestData.writeRowsInSpanner(
        spannerResourceManager, tables, unsupportedTables, KNOWN_MISMATCH_TABLES, true);

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(DATA_WAIT_MINUTES)),
                OracleDataTypesTestData.buildConditionCheck(
                    testUsername, tables, unsupportedTables, KNOWN_MISMATCH_TABLES));
    LOG.info("Wait finished with result {}", result);

    // Validate every table and collect all failures, so a single run reports all mismatches.
    List<String> failures = new ArrayList<>();
    OracleDataTypesTestData.validateSupportedTypes(
        jdbcResourceManager, testUsername, tables, KNOWN_MISMATCHES, failures);
    OracleDataTypesTestData.validateUnsupportedTypes(
        testUsername, unsupportedTables, KNOWN_MISMATCHES, failures);
    OracleDataTypesTestData.logDeadLetterQueue(gcsResourceManager);

    if (!failures.isEmpty()) {
      LOG.error(
          "{} data type validation failure(s):\n{}", failures.size(), String.join("\n", failures));
    }
    assertWithMessage("Data type validation failures:\n" + String.join("\n", failures))
        .that(failures)
        .isEmpty();
    assertThatResult(result).meetsConditions();
  }
}
