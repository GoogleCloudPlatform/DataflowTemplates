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

import static com.google.common.truth.Truth.assertWithMessage;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
import com.google.cloud.teleport.v2.templates.oracle.OracleDataTypesTestData.TypeTable;
import com.google.cloud.teleport.v2.templates.oracle.OracleDataTypesTestData.UnsupportedTable;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.OracleSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
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
 * Exhaustive data type integration test for the {@link DataStreamToSpanner} template with an Oracle
 * source (scenario {@code live-datatypes}), driven by the Oracle data type mapping matrix.
 *
 * <p>For every Oracle type and alias in the matrix the test covers: the default Spanner mapping
 * (scenario A), every alternative Spanner mapping (scenario B), the type as a primary key where
 * Oracle allows it (scenario C) and unsupported/complex types for row-count consistency (scenario
 * D), for both a GoogleSQL ({@link #testOracleDataTypes()}) and a PostgreSQL-dialect ({@link
 * #testOracleDataTypesPGDialect()}) Spanner database. Each test uses its own isolated Oracle user,
 * whose tables and rows are created by {@code oracle-schema.sql}; the Spanner tables are created by
 * {@code oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql} or {@code
 * oracle-POSTGRESQL-spanner-schema.sql}, under {@code
 * oracle/OracleDatastreamToSpannerDataTypesIT/}. The expected Spanner values are defined in {@link
 * OracleDataTypesTestData}.
 *
 * <p>Every value is replicated through both Datastream read paths: the rows inserted before the
 * stream is created are read by the backfill, then {@code oracle-cdc.sql} copies the rows of every
 * non-key table with their ids offset by {@link #CDC_ID_OFFSET}, which Datastream reads from the
 * redo logs (CDC, through LogMiner). Both copies must have the same expected values.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class OracleDatastreamToSpannerDataTypesIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(OracleDatastreamToSpannerDataTypesIT.class);

  private static final String RESOURCE_DIR = "oracle/OracleDatastreamToSpannerDataTypesIT/";
  private static final String ORACLE_DDL_RESOURCE = RESOURCE_DIR + "oracle-schema.sql";
  private static final String ORACLE_CDC_RESOURCE = RESOURCE_DIR + "oracle-cdc.sql";
  private static final String SPANNER_DDL_RESOURCE =
      RESOURCE_DIR + "oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";
  private static final String PG_DIALECT_SPANNER_DDL_RESOURCE =
      RESOURCE_DIR + "oracle-POSTGRESQL-spanner-schema.sql";
  private static final String GCS_PATH_PREFIX = "OracleDatastreamToSpannerDataTypesIT";

  /** Time to wait for the backfill of all tables to land in Spanner. */
  private static final int DATA_WAIT_MINUTES = 20;

  /** Time to wait for the CDC rows of all tables to land in Spanner. */
  private static final int CDC_WAIT_MINUTES = 15;

  /**
   * Offset added by {@code oracle-cdc.sql} to the ids of the rows it copies: the CDC copy of the
   * row with id {@code i} has id {@code CDC_ID_OFFSET + i}.
   */
  private static final int CDC_ID_OFFSET = 1000;

  /**
   * Tables whose mapping is known not to behave as the matrix expects, with the root cause. They
   * are still created, replicated and logged, but excluded from the wait condition and from the
   * assertions. See the Javadoc of {@link #testOracleDataTypes()}.
   */
  // TODO(b/579256484): Re-enable validation once these template conversions are fixed.
  private static final Map<String, String> KNOWN_MISMATCHES = knownMismatches();

  /**
   * {@link #KNOWN_MISMATCHES} of the PostgreSQL-dialect test: the same, plus the VARRAY table. An
   * Oracle VARRAY maps to Spanner {@code ARRAY<STRING(MAX)>}, PostgreSQL {@code varchar[]}: the
   * template writes NULL to a GoogleSQL ARRAY column, but has no conversion for a PostgreSQL array
   * column ({@code ChangeEventTypeConvertor#toValue} rejects {@code PG_ARRAY}), so these rows go to
   * the DLQ.
   */
  // TODO(b/579256484): Re-enable varray_table validation once PG_ARRAY handling is added.
  private static final Map<String, String> PG_DIALECT_KNOWN_MISMATCHES =
      ImmutableMap.<String, String>builder()
          .putAll(KNOWN_MISMATCHES)
          .put(
              "varray_table",
              "Template has no PostgreSQL array (PG_ARRAY) conversion (rows go to DLQ)")
          .build();

  /**
   * Tables with an Oracle LOB source column (BLOB, CLOB, NCLOB). The test does not enable
   * Datastream large-object streaming, so Datastream replicates these values as NULL; the tables
   * are still created and replicated, but their validation is disabled (excluded from the wait
   * condition and the value assertions). Their expected values are kept in {@link
   * OracleDataTypesTestData}. See the Javadoc of {@link #testOracleDataTypes()}.
   */
  // TODO(b/579256484): Enable stream_large_objects and re-enable LOB table validation.
  private static final Set<String> LOB_TABLES_NOT_VALIDATED =
      Set.of(
          "blob_table",
          "blob_to_str_table",
          "clob_table",
          "clob_to_bytes_table",
          "nclob_table",
          "nclob_to_bytes_table");

  private static Map<String, String> knownMismatches() {
    Map<String, String> m = new HashMap<>();
    // Text -> BYTES: Datastream emits character data as Avro strings, and the template decodes
    // every non-PostgreSQL BYTES value as hex (ChangeEventTypeConvertor#toByteArray), so text is
    // not converted to its UTF-8 bytes. Datastream gives no type information to tell text from RAW.
    String textToBytes = "Template decodes text as hex for BYTES columns (no UTF-8 conversion)";
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
            "nat_charvar")) {
      m.put(t + "_to_bytes_table", textToBytes);
    }
    // DATE/TIMESTAMP -> INT64: the template has no timestamp-to-integer conversion; these rows fail
    // conversion and are sent to the DLQ.
    String temporalToInt64 = "Template has no DATE/TIMESTAMP -> INT64 conversion (rows go to DLQ)";
    for (String t : List.of("date", "timestamp", "timestamp_tz", "timestamp_ltz")) {
      m.put(t + "_to_int64_table", temporalToInt64);
    }
    return Map.copyOf(m);
  }

  private static final HashSet<OracleDatastreamToSpannerDataTypesIT> testInstances =
      new HashSet<>();

  // GCS and Pub/Sub are shared by the tests, which use separate GCS paths. All the other resources
  // (Oracle user, Spanner database, Datastream stream, Dataflow job) belong to a single test and
  // are cleaned up as soon as that test completes, see runDataTypesTest.
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws Exception {
    skipBaseCleanup = true;
    synchronized (OracleDatastreamToSpannerDataTypesIT.class) {
      testInstances.add(this);
      if (gcsResourceManager == null) {
        try {
          setUpSharedResources();
        } catch (Exception e) {
          LOG.error("Setup failed, cleaning up the resources created so far.", e);
          try {
            cleanUpSharedResources(List.of(this));
          } catch (RuntimeException cleanupError) {
            e.addSuppressed(cleanupError);
          }
          throw e;
        }
      }
    }
  }

  /** Creates the resource managers shared by the tests. */
  private void setUpSharedResources() throws Exception {
    gcsResourceManager = setUpSpannerITGcsResourceManager();
    pubsubResourceManager = setUpPubSubResourceManager();
  }

  @AfterClass
  public static void cleanUp() {
    cleanUpSharedResources(testInstances);
  }

  /**
   * Cancels any Dataflow job left by the given test instances and cleans up the shared resource
   * managers, then resets them so that a later setup (after a failed one) starts clean.
   */
  private static void cleanUpSharedResources(
      Collection<OracleDatastreamToSpannerDataTypesIT> instances) {
    try {
      SharedOracleLiveITContainer.cleanUpTestResources(
          instances, List.of(), pubsubResourceManager, gcsResourceManager);
    } finally {
      pubsubResourceManager = null;
      gcsResourceManager = null;
    }
  }

  /**
   * KNOWN LIMITATIONS &amp; MATRIX DISCREPANCIES (Oracle -&gt; Datastream -&gt; Spanner, backfill
   * and CDC; tracked under b/579256484)
   *
   * <p>The tables below are still created and replicated, but are listed in {@link
   * #KNOWN_MISMATCHES} and excluded from the wait condition and value assertions:
   *
   * <ul>
   *   <li><b>Text -&gt; Spanner BYTES (b/579256484; 11 tables, e.g. {@code
   *       varchar2_to_bytes_table}, {@code nchar_to_bytes_table})</b>: Datastream emits Oracle
   *       character data (VARCHAR2, CHAR, NVARCHAR2, NCHAR and aliases) as Avro strings, and Oracle
   *       RAW is also emitted as an (uppercase hex) Avro string. The change event carries no type
   *       information to tell the two apart, so <code>ChangeEventTypeConvertor#toByteArray</code>
   *       hex-decodes every non-PostgreSQL string routed to a <code>BYTES</code> column. Text is
   *       therefore not converted to its UTF-8 bytes: strings that happen to be valid hex (e.g.
   *       {@code 'A'} or {@code 'DEADBEEF'}) are silently decoded to unrelated bytes, and other
   *       strings (e.g. {@code 'DROP TABLE'}) fail conversion. Users who need text in Spanner
   *       <code>BYTES
   *       </code> should use a custom transformation to encode the text.
   *   <li><b>DATE/TIMESTAMP -&gt; Spanner INT64 (b/579256484; 4 tables: {@code date}, {@code
   *       timestamp}, {@code timestamp_tz}, {@code timestamp_ltz} {@code _to_int64_table})</b>: the
   *       template has no temporal-to-integer (e.g. epoch) conversion, so these rows fail
   *       conversion and are written to the dead-letter queue instead of Spanner.
   * </ul>
   *
   * <p><b>LOB validation is disabled (b/579256484; 6 tables, {@link #LOB_TABLES_NOT_VALIDATED}:
   * {@code blob_table}, {@code blob_to_str_table}, {@code clob_table}, {@code clob_to_bytes_table},
   * {@code nclob_table}, {@code nclob_to_bytes_table})</b>: the test does not enable Datastream
   * large-object streaming ({@code OracleSourceConfig.stream_large_objects}), and without it
   * Datastream replicates BLOB, CLOB and NCLOB values as <code>NULL</code>. These tables are
   * created and replicated, but excluded from the wait condition and the value assertions; their
   * expected values are kept in {@link OracleDataTypesTestData} so validation can be re-enabled.
   * When LOB streaming is enabled, these known issues apply:
   *
   * <ul>
   *   <li><b>BLOB -&gt; Spanner BYTES &mdash; open bug (b/579256484)</b>: Datastream emits BLOB as
   *       Avro <code>bytes</code>, which the template writes into the change event as base64, but
   *       <code>ChangeEventTypeConvertor#toByteArray</code> hex-decodes every non-PostgreSQL <code>
   *       BYTES</code> value, so the data is silently corrupted (e.g. {@code DEADBEEF} is stored as
   *       {@code 2F1F6FEF}).
   *   <li><b>CLOB/NCLOB -&gt; Spanner BYTES (b/579256484)</b>: same text -&gt; BYTES limitation as
   *       above.
   * </ul>
   *
   * <p><b>TIMESTAMP WITH TIME ZONE / WITH LOCAL TIME ZONE values are restricted</b> (all {@code
   * timestamp_tz*} and {@code timestamp_ltz*} tables) to avoid two open bugs; the matrix edge cases
   * that would hit them are not validated:
   *
   * <ul>
   *   <li><b>Non-UTC offsets are stored as the wrong instant &mdash; open bug (b/579256484)</b>:
   *       for Oracle, Datastream puts the local wall-clock time in the {@code
   *       timestampTz.timestamp} field and the offset separately, but <code>
   *       FormatDatastreamRecordToJson</code> treats {@code timestamp} as the instant, so the
   *       offset is ignored (e.g. {@code 10:30 +05:30} is stored as {@code 10:30Z} instead of
   *       {@code 05:00Z}). Only UTC values ({@code +00:00} / {@code UTC}) are validated; the test
   *       database and session time zone are {@code +00:00}.
   *   <li><b>Values outside ~1677-09-21..2262-04-11 overflow &mdash; open bug (b/579256484)</b>:
   *       <code>FormatDatastreamRecordToJson</code> converts the microseconds to nanoseconds in a
   *       {@code long}, which overflows (e.g. {@code 0001-01-01} is stored as {@code
   *       1754-08-30T22:43:41Z}). This affects every Datastream source that sends {@code
   *       timestampTz}. The matrix extremes {@code 0001-01-01} / {@code 9999-12-31} are replaced by
   *       {@code 1700-01-01} / {@code 2200-12-31}.
   * </ul>
   *
   * <p>Other behaviours that differ from the matrix but are asserted as expected:
   *
   * <ul>
   *   <li><b>RAW -&gt; Spanner STRING is uppercase hex, not Base64</b>: Datastream delivers RAW as
   *       an uppercase hex string, which is stored verbatim (e.g. {@code HEXTORAW('DEADBEEF')}
   *       -&gt; {@code "DEADBEEF"}), the same as MySQL binary -&gt; STRING.
   *   <li><b>Unsupported types (scenario D)</b>: types Datastream cannot replicate are only checked
   *       for row-count consistency.
   * </ul>
   */
  @Test
  public void testOracleDataTypes() throws Exception {
    runDataTypesTest(
        Dialect.GOOGLE_STANDARD_SQL,
        SPANNER_DDL_RESOURCE,
        OracleDataTypesTestData.tables(),
        OracleDataTypesTestData.unsupportedTables(),
        KNOWN_MISMATCHES,
        GCS_PATH_PREFIX);
  }

  /**
   * Same as {@link #testOracleDataTypes()}, with a PostgreSQL-dialect Spanner database. The known
   * limitations described there apply too, plus {@link #PG_DIALECT_KNOWN_MISMATCHES}.
   */
  @Test
  public void testOracleDataTypesPGDialect() throws Exception {
    runDataTypesTest(
        Dialect.POSTGRESQL,
        PG_DIALECT_SPANNER_DDL_RESOURCE,
        OracleDataTypesTestData.postgresTables(),
        OracleDataTypesTestData.postgresUnsupportedTables(),
        PG_DIALECT_KNOWN_MISMATCHES,
        GCS_PATH_PREFIX + "-pg-dialect");
  }

  /**
   * Runs the data type test for a Spanner dialect with the resources of this test only (an isolated
   * Oracle user, a Spanner database, a Datastream stream and a Dataflow job), which are cleaned up
   * as soon as the test completes: the Dataflow job first, then the stream, the Spanner database
   * and finally the Oracle user.
   */
  private void runDataTypesTest(
      Dialect dialect,
      String spannerDdlResource,
      List<TypeTable> tables,
      List<UnsupportedTable> unsupportedTables,
      Map<String, String> knownMismatches,
      String gcsPathPrefix)
      throws Exception {
    SpannerResourceManager spanner = null;
    DatastreamResourceManager datastream = null;
    String oracleUser = null;
    Throwable failure = null;
    try {
      spanner =
          dialect == Dialect.POSTGRESQL
              ? setUpPGDialectSpannerResourceManager()
              : setUpSpannerResourceManager();
      datastream =
          DatastreamResourceManager.builder(testName, PROJECT, REGION)
              .setCredentialsProvider(credentialsProvider)
              .setPrivateConnectivity(
                  System.getProperty("privateConnectivity", "datastream-connect-2"))
              .build();
      oracleUser = SharedOracleLiveITContainer.setupOracleIsolatedUser();
      replicateAndValidate(
          spanner,
          datastream,
          oracleUser,
          spannerDdlResource,
          tables,
          unsupportedTables,
          knownMismatches,
          gcsPathPrefix);
    } catch (Throwable t) {
      failure = t;
      throw t;
    } finally {
      try {
        SharedOracleLiveITContainer.cleanUpTestResources(
            List.of(this), Arrays.asList(oracleUser), datastream, spanner);
      } catch (RuntimeException cleanupError) {
        if (failure == null) {
          throw cleanupError;
        }
        failure.addSuppressed(cleanupError);
      }
    }
  }

  /**
   * Creates the tables and rows of {@code oracle-schema.sql} in the given Oracle user and the
   * Spanner tables, replicates them with a Datastream stream and a Dataflow job (backfill, then
   * CDC) and validates every table.
   */
  private void replicateAndValidate(
      SpannerResourceManager spanner,
      DatastreamResourceManager datastream,
      String oracleUser,
      String spannerDdlResource,
      List<TypeTable> tables,
      List<UnsupportedTable> unsupportedTables,
      Map<String, String> knownMismatches,
      String gcsPathPrefix)
      throws Exception {
    LOG.info("Creating Oracle schema and data in {}", oracleUser);
    SharedOracleLiveITContainer.executeSqlScript(oracleUser, ORACLE_DDL_RESOURCE);

    LOG.info("Creating Spanner DDL {}...", spannerDdlResource);
    createSpannerDDL(spanner, spannerDdlResource);

    OracleSource oracleSource =
        SharedOracleLiveITContainer.oracleSourceBuilder(oracleUser)
            .setAllowedTables(
                Map.of(oracleUser.toUpperCase(), getAllowedTables(tables, unsupportedTables)))
            .build();

    LOG.info("Launching Dataflow job...");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            gcsPathPrefix,
            null,
            null,
            gcsPathPrefix,
            spanner,
            pubsubResourceManager,
            new HashMap<>(),
            null,
            null,
            gcsResourceManager,
            datastream,
            null,
            oracleSource);
    assertThatPipeline(jobInfo).isRunning();

    LOG.info("Waiting for the backfill of all tables...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(DATA_WAIT_MINUTES)),
                buildConditionCheck(spanner, tables, unsupportedTables, knownMismatches, false));
    LOG.info("Backfill wait finished with result {}", result);

    // CDC phase: only once the backfill is complete, so that the new rows are read from the redo
    // logs and not by the backfill.
    boolean cdcApplied = result == PipelineOperator.Result.CONDITION_MET;
    if (cdcApplied) {
      LOG.info("Copying the rows of the non-key tables in Oracle (CDC)...");
      SharedOracleLiveITContainer.executeSqlScript(oracleUser, ORACLE_CDC_RESOURCE);
      SharedOracleLiveITContainer.flushRedoLogs();
      result =
          pipelineOperator()
              .waitForCondition(
                  createConfig(jobInfo, Duration.ofMinutes(CDC_WAIT_MINUTES)),
                  buildConditionCheck(spanner, tables, unsupportedTables, knownMismatches, true));
      LOG.info("CDC wait finished with result {}", result);
    }

    // Validate every table and collect all failures, so a single run reports all mismatches.
    List<String> failures = new ArrayList<>();
    validateSupportedTypes(spanner, tables, knownMismatches, cdcApplied, failures);
    validateUnsupportedTypes(spanner, unsupportedTables, knownMismatches, failures);
    logDeadLetterQueue(gcsPathPrefix);

    if (!failures.isEmpty()) {
      LOG.error(
          "{} data type validation failure(s):\n{}", failures.size(), String.join("\n", failures));
    }
    assertWithMessage("Data type validation failures:\n" + String.join("\n", failures))
        .that(failures)
        .isEmpty();
    assertWithMessage("Pipeline wait result")
        .that(result)
        .isEqualTo(PipelineOperator.Result.CONDITION_MET);
  }

  private static List<String> getAllowedTables(
      List<TypeTable> tables, List<UnsupportedTable> unsupportedTables) {
    List<String> names = new ArrayList<>();
    tables.forEach(t -> names.add(t.table()));
    unsupportedTables.forEach(t -> names.add(t.table()));
    return names;
  }

  /**
   * Waits for every validated table to have all its backfill rows and, if {@code cdcApplied}, all
   * the rows copied by {@code oracle-cdc.sql}.
   */
  private static ConditionCheck buildConditionCheck(
      SpannerResourceManager spanner,
      List<TypeTable> tables,
      List<UnsupportedTable> unsupportedTables,
      Map<String, String> knownMismatches,
      boolean cdcApplied) {
    ConditionCheck combined = null;
    for (TypeTable t : tables) {
      if (knownMismatches.containsKey(t.table()) || LOB_TABLES_NOT_VALIDATED.contains(t.table())) {
        continue;
      }
      combined = and(combined, rowsCheck(spanner, t.table(), expectedRowCount(t, cdcApplied)));
    }
    for (UnsupportedTable t : unsupportedTables) {
      if (knownMismatches.containsKey(t.table())) {
        continue;
      }
      combined = and(combined, rowsCheck(spanner, t.table(), t.rowCount()));
    }
    return combined;
  }

  /** Rows expected in a table: the CDC copies the rows of the non-key tables. */
  private static int expectedRowCount(TypeTable t, boolean cdcApplied) {
    return t.values().size() * (cdcApplied && !t.pk() ? 2 : 1);
  }

  private static ConditionCheck rowsCheck(SpannerResourceManager spanner, String table, int rows) {
    return SpannerRowsCheck.builder(spanner, table).setMinRows(rows).build();
  }

  private static ConditionCheck and(ConditionCheck a, ConditionCheck b) {
    return a == null ? b : a.and(b);
  }

  /**
   * Validates scenarios A, B and C: every expected row and value must be present in Spanner, for
   * the rows of the non-key tables also their CDC copies if {@code cdcApplied}.
   */
  private static void validateSupportedTypes(
      SpannerResourceManager spanner,
      List<TypeTable> tables,
      Map<String, String> knownMismatches,
      boolean cdcApplied,
      List<String> failures) {
    for (TypeTable t : tables) {
      String table = t.table();
      String column = t.column();
      if (LOB_TABLES_NOT_VALIDATED.contains(table)) {
        // Validation disabled: LOB streaming is not enabled on the Datastream stream, so the LOB
        // values are replicated as NULL. See the Javadoc of testOracleDataTypes.
        LOG.info("SKIPPED {} (LOB values are not streamed by Datastream; not validated)", table);
        continue;
      }
      boolean pk = t.pk();
      boolean known = knownMismatches.containsKey(table);
      List<String> tableFailures = new ArrayList<>();
      List<Struct> rows;
      try {
        rows =
            pk
                ? spanner.readTableRecords(table, column)
                : spanner.readTableRecords(table, "id", column);
      } catch (Exception e) {
        tableFailures.add("read failed: " + e.getMessage());
        rows = List.of();
      }
      for (Struct row : rows) {
        LOG.info(
            "ACTUAL {} {}={}",
            table,
            pk ? "pk" : "id=" + row.getLong("id") + " " + column,
            describe(row, column));
      }
      List<String> expectedValues = t.values();
      int expectedRows = expectedRowCount(t, cdcApplied);
      if (rows.size() != expectedRows) {
        tableFailures.add(
            String.format("expected %d rows but found %d", expectedRows, rows.size()));
      }
      if (pk) {
        List<Struct> remaining = new ArrayList<>(rows);
        for (String exp : expectedValues) {
          Struct match = null;
          for (Struct s : remaining) {
            if (matches(exp, s, column)) {
              match = s;
              break;
            }
          }
          if (match == null) {
            tableFailures.add("missing pk value " + abbreviate(exp));
          } else {
            remaining.remove(match);
          }
        }
      } else {
        Map<Long, Struct> byId = new HashMap<>();
        rows.forEach(s -> byId.put(s.getLong("id"), s));
        // Non-key tables use ids 1..n, in the order of the expected values (backfill), and
        // CDC_ID_OFFSET + 1..n for their CDC copies.
        List<Integer> idOffsets = cdcApplied ? List.of(0, CDC_ID_OFFSET) : List.of(0);
        for (int offset : idOffsets) {
          for (int i = 0; i < expectedValues.size(); i++) {
            long id = offset + i + 1;
            String exp = expectedValues.get(i);
            Struct s = byId.get(id);
            if (s == null) {
              tableFailures.add("id=" + id + " missing (expected " + abbreviate(exp) + ")");
            } else if (!matches(exp, s, column)) {
              tableFailures.add(
                  "id=" + id + " expected " + abbreviate(exp) + " but was " + describe(s, column));
            }
          }
        }
      }
      for (String f : tableFailures) {
        String msg = table + " [" + t.sourceType() + " -> " + t.spannerType() + "]: " + f;
        if (known) {
          LOG.warn("KNOWN MISMATCH (ignored: {}) {}", knownMismatches.get(table), msg);
        } else {
          failures.add(msg);
        }
      }
      if (tableFailures.isEmpty()) {
        LOG.info("PASS {} ({} rows)", table, rows.size());
      }
    }
  }

  /**
   * Validates scenario D: unsupported/complex types must not break replication; every source row
   * must be migrated (row-count consistency).
   */
  private static void validateUnsupportedTypes(
      SpannerResourceManager spanner,
      List<UnsupportedTable> unsupportedTables,
      Map<String, String> knownMismatches,
      List<String> failures) {
    for (UnsupportedTable t : unsupportedTables) {
      String table = t.table();
      String column = t.column();
      long expectedCount = t.rowCount();
      long actual;
      try {
        actual = spanner.getRowCount(table);
        for (Struct row : spanner.readTableRecords(table, "id", column)) {
          LOG.info(
              "ACTUAL {} id={} {}={}", table, row.getLong("id"), column, describe(row, column));
        }
      } catch (Exception e) {
        failures.add(table + ": read failed: " + e.getMessage());
        continue;
      }
      if (actual != expectedCount) {
        String msg =
            String.format(
                "%s [%s, unsupported]: expected %d rows but found %d",
                table, t.sourceType(), expectedCount, actual);
        if (knownMismatches.containsKey(table)) {
          LOG.warn("KNOWN MISMATCH (ignored: {}) {}", knownMismatches.get(table), msg);
        } else {
          failures.add(msg);
        }
      } else {
        LOG.info("PASS {} ({} rows, unsupported type)", table, actual);
      }
    }
  }

  /** Logs dead-letter queue files written by the pipeline, to explain missing rows. */
  private static void logDeadLetterQueue(String gcsPathPrefix) {
    try {
      List<Artifact> artifacts =
          gcsResourceManager.listArtifacts(gcsPathPrefix + "/dlq/", Pattern.compile(".*"));
      LOG.info("Found {} DLQ file(s)", artifacts.size());
      for (Artifact a : artifacts) {
        String contents = new String(a.contents(), StandardCharsets.UTF_8);
        LOG.info(
            "DLQ file {} ({} bytes): {}", a.name(), contents.length(), abbreviate(contents, 4000));
      }
    } catch (Exception e) {
      LOG.warn("Unable to list DLQ files", e);
    }
  }

  /** Type-aware comparison of an expected value against a Spanner column value. */
  private static boolean matches(String expected, Struct row, String column) {
    if (expected == null) {
      return row.isNull(column);
    }
    if (row.isNull(column)) {
      return false;
    }
    try {
      switch (row.getColumnType(column).getCode()) {
        case STRING:
          return expected.equals(row.getString(column));
        case BYTES:
          return expected.equals(row.getBytes(column).toBase64());
        case NUMERIC:
          return new BigDecimal(expected).compareTo(row.getBigDecimal(column)) == 0;
        case PG_NUMERIC:
          return new BigDecimal(expected)
                  .compareTo(new BigDecimal(row.getValue(column).getString()))
              == 0;
        case FLOAT64:
          return Double.valueOf(expected).equals(row.getDouble(column));
        case FLOAT32:
          return Float.valueOf(expected).equals(row.getFloat(column));
        case INT64:
          return Long.parseLong(expected) == row.getLong(column);
        case TIMESTAMP:
          return Timestamp.parseTimestamp(expected).equals(row.getTimestamp(column));
        case DATE:
          return Date.parseDate(expected).equals(row.getDate(column));
        case JSON:
          return expected.equals(row.getJson(column));
        case PG_JSONB:
          return expected.equals(row.getPgJsonb(column));
        default:
          return expected.equals(row.getValue(column).toString());
      }
    } catch (RuntimeException e) {
      return false;
    }
  }

  private static String describe(Struct row, String column) {
    if (row.isNull(column)) {
      return "NULL";
    }
    String value;
    switch (row.getColumnType(column).getCode()) {
      case STRING:
        value = row.getString(column);
        break;
      case BYTES:
        value = "b64:" + row.getBytes(column).toBase64();
        break;
      case NUMERIC:
        value = row.getBigDecimal(column).toPlainString();
        break;
      case JSON:
        value = row.getJson(column);
        break;
      default:
        value = row.getValue(column).toString();
    }
    return abbreviate(value);
  }

  private static String abbreviate(String s) {
    return abbreviate(s, 120);
  }

  private static String abbreviate(String s, int max) {
    if (s == null) {
      return "NULL";
    }
    String quoted = "'" + s + "'";
    return quoted.length() <= max
        ? quoted
        : quoted.substring(0, max) + "...(length=" + s.length() + ")";
  }
}
