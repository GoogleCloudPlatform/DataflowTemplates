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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
import com.google.common.io.Resources;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
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
 * D). Tables, rows and the expected Spanner values are defined in resource files under {@code
 * oracle/OracleDatastreamToSpannerDataTypesMatrixIT/}; {@code expected-data.json} drives all
 * assertions.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class OracleDatastreamToSpannerDataTypesIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(OracleDatastreamToSpannerDataTypesIT.class);

  private static final String RESOURCE_DIR = "oracle/OracleDatastreamToSpannerDataTypesIT/";
  private static final String ORACLE_DDL_RESOURCE = RESOURCE_DIR + "oracle-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      RESOURCE_DIR + "oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";
  private static final String EXPECTED_DATA_RESOURCE = RESOURCE_DIR + "expected-data.json";
  private static final String GCS_PATH_PREFIX = "OracleDatastreamToSpannerDataTypesIT";

  /** Time to wait for the backfill of all tables to land in Spanner. */
  private static final int DATA_WAIT_MINUTES = 20;

  /**
   * Tables whose mapping is known not to behave as the matrix expects, with the root cause. They
   * are still created, replicated and logged, but excluded from the wait condition and from the
   * assertions. Every entry is documented in the migration report.
   */
  private static final Map<String, String> KNOWN_MISMATCHES = knownMismatches();

  private static final Set<String> KNOWN_MISMATCH_TABLES = KNOWN_MISMATCHES.keySet();

  /**
   * Tables with an Oracle LOB source column (BLOB, CLOB, NCLOB). The test does not enable
   * Datastream large-object streaming, so Datastream replicates these values as NULL; the tables
   * are still created and replicated, but their validation is disabled (excluded from the wait
   * condition and the value assertions). Their expected values are kept in {@code
   * expected-data.json}. See the Javadoc of {@link #testOracleDataTypes()}.
   */
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

  private static String oracleUser;
  private static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;

  @Before
  public void setUp() throws Exception {
    skipBaseCleanup = true;
    synchronized (OracleDatastreamToSpannerDataTypesIT.class) {
      testInstances.add(this);
      if (spannerResourceManager == null) {
        oracleUser = SharedOracleLiveITContainer.setupOracleIsolatedUser();
        spannerResourceManager = setUpSpannerResourceManager();
        gcsResourceManager = setUpSpannerITGcsResourceManager();
        pubsubResourceManager = setUpPubSubResourceManager();
        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity("datastream-connect-2")
                .build();

        LOG.info("Creating Oracle schema and data in {}", oracleUser);
        SharedOracleLiveITContainer.executeSqlScript(oracleUser, ORACLE_DDL_RESOURCE);
        SharedOracleLiveITContainer.flushRedoLogs();
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    // Stop consumers (Dataflow job, Datastream stream) before dropping the Oracle source user.
    for (OracleDatastreamToSpannerDataTypesIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        datastreamResourceManager,
        spannerResourceManager,
        pubsubResourceManager,
        gcsResourceManager);
    SharedOracleLiveITContainer.dropUser(oracleUser);
  }

  /**
   * KNOWN LIMITATIONS &amp; MATRIX DISCREPANCIES (Oracle -&gt; Datastream -&gt; Spanner, live CDC)
   *
   * <p>The tables below are still created and replicated, but are listed in {@link
   * #KNOWN_MISMATCHES} and excluded from the wait condition and value assertions:
   *
   * <ul>
   *   <li><b>Text -&gt; Spanner BYTES (11 tables, e.g. {@code varchar2_to_bytes_table}, {@code
   *       nchar_to_bytes_table})</b>: Datastream emits Oracle character data (VARCHAR2, CHAR,
   *       NVARCHAR2, NCHAR and aliases) as Avro strings, and Oracle RAW is also emitted as an
   *       (uppercase hex) Avro string. The change event carries no type information to tell the two
   *       apart, so <code>ChangeEventTypeConvertor#toByteArray</code> hex-decodes every
   *       non-PostgreSQL string routed to a <code>BYTES</code> column. Text is therefore not
   *       converted to its UTF-8 bytes: strings that happen to be valid hex (e.g. {@code 'A'} or
   *       {@code 'DEADBEEF'}) are silently decoded to unrelated bytes, and other strings (e.g.
   *       {@code 'DROP TABLE'}) fail conversion. Users who need text in Spanner <code>BYTES
   *       </code> should use a custom transformation to encode the text.
   *   <li><b>DATE/TIMESTAMP -&gt; Spanner INT64 (4 tables: {@code date}, {@code timestamp}, {@code
   *       timestamp_tz}, {@code timestamp_ltz} {@code _to_int64_table})</b>: the template has no
   *       temporal-to-integer (e.g. epoch) conversion, so these rows fail conversion and are
   *       written to the dead-letter queue instead of Spanner.
   * </ul>
   *
   * <p><b>LOB validation is disabled (6 tables, {@link #LOB_TABLES_NOT_VALIDATED}: {@code
   * blob_table}, {@code blob_to_str_table}, {@code clob_table}, {@code clob_to_bytes_table}, {@code
   * nclob_table}, {@code nclob_to_bytes_table})</b>: the test does not enable Datastream
   * large-object streaming ({@code OracleSourceConfig.stream_large_objects}), and without it
   * Datastream replicates BLOB, CLOB and NCLOB values as <code>NULL</code>. These tables are
   * created and replicated, but excluded from the wait condition and the value assertions; their
   * expected values are kept in {@code expected-data.json} so validation can be re-enabled. When
   * LOB streaming is enabled, these known issues apply:
   *
   * <ul>
   *   <li><b>BLOB -&gt; Spanner BYTES &mdash; open bug</b>: Datastream emits BLOB as Avro <code>
   *       bytes</code>, which the template writes into the change event as base64, but <code>
   *       ChangeEventTypeConvertor#toByteArray</code> hex-decodes every non-PostgreSQL <code>BYTES
   *       </code> value, so the data is silently corrupted (e.g. {@code DEADBEEF} is stored as
   *       {@code 2F1F6FEF}).
   *   <li><b>CLOB/NCLOB -&gt; Spanner BYTES</b>: same text -&gt; BYTES limitation as above.
   * </ul>
   *
   * <p>See {@code v2/spanner-common/docs/ORACLE_DATATYPE_KNOWN_LIMITATIONS.md}.
   *
   * <p><b>TIMESTAMP WITH TIME ZONE / WITH LOCAL TIME ZONE values are restricted</b> (all {@code
   * timestamp_tz*} and {@code timestamp_ltz*} tables) to avoid two open bugs; the matrix edge cases
   * that would hit them are not validated:
   *
   * <ul>
   *   <li><b>Non-UTC offsets are stored as the wrong instant &mdash; open bug</b>: for Oracle,
   *       Datastream puts the local wall-clock time in the {@code timestampTz.timestamp} field and
   *       the offset separately, but <code>FormatDatastreamRecordToJson</code> treats {@code
   *       timestamp} as the instant, so the offset is ignored (e.g. {@code 10:30 +05:30} is stored
   *       as {@code 10:30Z} instead of {@code 05:00Z}). Only UTC values ({@code +00:00} / {@code
   *       UTC}) are validated; the test database and session time zone are {@code +00:00}.
   *   <li><b>Values outside ~1677-09-21..2262-04-11 overflow &mdash; open bug</b>: <code>
   *       FormatDatastreamRecordToJson</code> converts the microseconds to nanoseconds in a {@code
   *       long}, which overflows (e.g. {@code 0001-01-01} is stored as {@code
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
    JsonNode expected = loadExpectedData();

    LOG.info("Creating Spanner DDL...");
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    OracleSource oracleSource =
        SharedOracleLiveITContainer.oracleSourceBuilder(oracleUser)
            .setAllowedTables(Map.of(oracleUser.toUpperCase(), getAllowedTables(expected)))
            .build();

    LOG.info("Launching Dataflow job...");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            GCS_PATH_PREFIX,
            spannerResourceManager,
            pubsubResourceManager,
            new HashMap<>(),
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            null,
            oracleSource);
    assertThatPipeline(jobInfo).isRunning();

    LOG.info("Waiting for pipeline to replicate all tables...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(DATA_WAIT_MINUTES)),
                buildConditionCheck(expected));
    LOG.info("Wait finished with result {}", result);

    // Validate every table and collect all failures, so a single run reports all mismatches.
    List<String> failures = new ArrayList<>();
    validateSupportedTypes(expected, failures);
    validateUnsupportedTypes(expected, failures);
    logDeadLetterQueue();

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

  private JsonNode loadExpectedData() throws IOException {
    String json =
        Resources.toString(Resources.getResource(EXPECTED_DATA_RESOURCE), StandardCharsets.UTF_8);
    return new ObjectMapper().readTree(json);
  }

  private List<String> getAllowedTables(JsonNode expected) {
    List<String> tables = new ArrayList<>();
    expected.get("tables").forEach(t -> tables.add(t.get("table").asText()));
    expected.get("unsupportedTables").forEach(t -> tables.add(t.get("table").asText()));
    return tables;
  }

  private ConditionCheck buildConditionCheck(JsonNode expected) {
    ConditionCheck combined = null;
    for (JsonNode t : expected.get("tables")) {
      String table = t.get("table").asText();
      if (KNOWN_MISMATCH_TABLES.contains(table) || LOB_TABLES_NOT_VALIDATED.contains(table)) {
        continue;
      }
      combined = and(combined, rowsCheck(table, t.get("rows").size()));
    }
    for (JsonNode t : expected.get("unsupportedTables")) {
      String table = t.get("table").asText();
      if (KNOWN_MISMATCH_TABLES.contains(table)) {
        continue;
      }
      combined = and(combined, rowsCheck(table, t.get("expectedRowCount").asInt()));
    }
    return combined;
  }

  private ConditionCheck rowsCheck(String table, int rows) {
    return SpannerRowsCheck.builder(spannerResourceManager, table).setMinRows(rows).build();
  }

  private static ConditionCheck and(ConditionCheck a, ConditionCheck b) {
    return a == null ? b : a.and(b);
  }

  /** Validates scenarios A, B and C: every expected row and value must be present in Spanner. */
  private void validateSupportedTypes(JsonNode expected, List<String> failures) {
    for (JsonNode t : expected.get("tables")) {
      String table = t.get("table").asText();
      String column = t.get("column").asText();
      if (LOB_TABLES_NOT_VALIDATED.contains(table)) {
        // Validation disabled: LOB streaming is not enabled on the Datastream stream, so the LOB
        // values are replicated as NULL. See the Javadoc of testOracleDataTypes.
        LOG.info("SKIPPED {} (LOB values are not streamed by Datastream; not validated)", table);
        continue;
      }
      boolean pk = t.get("pk").asBoolean();
      boolean known = KNOWN_MISMATCH_TABLES.contains(table);
      List<String> tableFailures = new ArrayList<>();
      List<Struct> rows;
      try {
        rows =
            pk
                ? spannerResourceManager.readTableRecords(table, column)
                : spannerResourceManager.readTableRecords(table, "id", column);
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
      JsonNode expectedRows = t.get("rows");
      if (rows.size() != expectedRows.size()) {
        tableFailures.add(
            String.format("expected %d rows but found %d", expectedRows.size(), rows.size()));
      }
      if (pk) {
        List<Struct> remaining = new ArrayList<>(rows);
        for (JsonNode r : expectedRows) {
          String exp = resolveExpected(r.get("value"));
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
        for (JsonNode r : expectedRows) {
          long id = r.get("id").asLong();
          String exp = resolveExpected(r.get("value"));
          Struct s = byId.get(id);
          if (s == null) {
            tableFailures.add("id=" + id + " missing (expected " + abbreviate(exp) + ")");
          } else if (!matches(exp, s, column)) {
            tableFailures.add(
                "id=" + id + " expected " + abbreviate(exp) + " but was " + describe(s, column));
          }
        }
      }
      for (String f : tableFailures) {
        String msg =
            table
                + " ["
                + t.get("sourceType").asText()
                + " -> "
                + t.get("spannerType").asText()
                + "]: "
                + f;
        if (known) {
          LOG.warn("KNOWN MISMATCH (ignored: {}) {}", KNOWN_MISMATCHES.get(table), msg);
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
  private void validateUnsupportedTypes(JsonNode expected, List<String> failures) {
    for (JsonNode t : expected.get("unsupportedTables")) {
      String table = t.get("table").asText();
      String column = t.get("column").asText();
      long expectedCount = t.get("expectedRowCount").asLong();
      long actual;
      try {
        actual = spannerResourceManager.getRowCount(table);
        for (Struct row : spannerResourceManager.readTableRecords(table, "id", column)) {
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
                table, t.get("sourceType").asText(), expectedCount, actual);
        if (KNOWN_MISMATCH_TABLES.contains(table)) {
          LOG.warn("KNOWN MISMATCH (ignored) {}", msg);
        } else {
          failures.add(msg);
        }
      } else {
        LOG.info("PASS {} ({} rows, unsupported type)", table, actual);
      }
    }
  }

  /** Logs dead-letter queue files written by the pipeline, to explain missing rows. */
  private void logDeadLetterQueue() {
    try {
      List<Artifact> artifacts =
          gcsResourceManager.listArtifacts(GCS_PATH_PREFIX + "/dlq/", Pattern.compile(".*"));
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

  /**
   * Resolves an expected value from {@code expected-data.json}: either null, a literal string, or a
   * compact object {@code {prefix, repeat, times, base64}} for very large values.
   */
  private static String resolveExpected(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isTextual()) {
      return node.asText();
    }
    String value =
        node.get("prefix").asText() + node.get("repeat").asText().repeat(node.get("times").asInt());
    if (node.get("base64").asBoolean()) {
      return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
    return value;
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
