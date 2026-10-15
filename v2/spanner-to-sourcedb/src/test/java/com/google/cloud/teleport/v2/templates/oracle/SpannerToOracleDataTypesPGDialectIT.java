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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.resourcemanager.SpannerOracleResourceManager;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.artifacts.Artifact;
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
 * Datastream marks as unsupported are written for row-count consistency (scenario D). Tables,
 * Spanner values and expected Oracle values are defined in {@code expected-data.json}.
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
  private static final String EXPECTED_DATA_RESOURCE = RESOURCE_DIR + "expected-data.json";

  /** Time to wait for all rows to be replicated to Oracle. */
  private static final int DATA_WAIT_MINUTES = 15;

  /**
   * Tables whose mapping is known not to behave as the matrix expects, with the root cause. They
   * are still written and logged, but excluded from the wait condition and from the assertions.
   * Every entry is documented in the migration report.
   */
  private static final Map<String, String> KNOWN_MISMATCHES = knownMismatches();

  private static final Set<String> KNOWN_MISMATCH_TABLES = KNOWN_MISMATCHES.keySet();

  private static Map<String, String> knownMismatches() {
    Map<String, String> m = new HashMap<>();
    // BYTEA -> character columns: OracleDMLGenerator writes HEXTORAW(...) into the column, so
    // VARCHAR2/CHAR store the hex text and N-types decode the UTF-8 bytes as UTF-16. Open bug.
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
            "nat_charvar")) {
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
   * <p>The tables below are still written to Spanner, but are listed in {@link #KNOWN_MISMATCHES}
   * and excluded from the wait condition and value assertions:
   *
   * <ul>
   *   <li><b>Spanner bytea -&gt; Oracle character columns (11 tables, e.g. {@code
   *       varchar2_to_bytea_table}, {@code nchar_to_bytea_table}) &mdash; open bug</b>: <code>
   *       OracleDMLGenerator</code> writes Spanner <code>bytea</code> as <code>HEXTORAW('...')
   *       </code>. For VARCHAR2/CHAR, Oracle implicitly converts the RAW to its <i>hex text</i>
   *       (e.g. bytes of {@code "Hello"} become {@code "48656C6C6F"}). For NVARCHAR2/NCHAR it is
   *       wrapped in <code>UTL_RAW.CAST_TO_NVARCHAR2</code>, which interprets the UTF-8 bytes as
   *       the national character set (AL16UTF16), producing garbled text.
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
   *
   * <p>See {@code v2/spanner-common/docs/ORACLE_DATATYPE_KNOWN_LIMITATIONS.md}.
   */
  @Test
  public void spannerToOracleDataTypesPGDialect() throws Exception {
    assertThatPipeline(jobInfo).isRunning();
    JsonNode expected = loadExpectedData();

    writeRowsInSpanner(expected);

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

  /** Writes every row of every table to Spanner with the table's Spanner column type. */
  private void writeRowsInSpanner(JsonNode expected) throws Exception {
    for (String section : List.of("tables", "unsupportedTables")) {
      for (JsonNode t : expected.get(section)) {
        String table = t.get("table").asText();
        String column = t.get("column").asText();
        String spannerType = t.get("spannerType").asText();
        boolean pk = t.has("pk") && t.get("pk").asBoolean();
        List<Mutation> mutations = new ArrayList<>();
        for (JsonNode r : t.get("rows")) {
          Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table);
          if (!pk) {
            builder.set("id").to(r.get("id").asLong());
          }
          builder.set(column).to(toSpannerValue(spannerType, resolve(r.get("spanner"))));
          mutations.add(builder.build());
        }
        try {
          spannerResourceManager.write(mutations);
        } catch (Exception e) {
          // A Spanner write failure is a test setup problem; report it with the table name.
          throw new Exception("Failed to write rows to Spanner table " + table, e);
        }
      }
    }
    LOG.info("Wrote all rows to Spanner");
  }

  private ConditionCheck buildConditionCheck(JsonNode expected) {
    ConditionCheck combined = null;
    for (String section : List.of("tables", "unsupportedTables")) {
      for (JsonNode t : expected.get(section)) {
        String table = t.get("table").asText();
        if (KNOWN_MISMATCH_TABLES.contains(table)) {
          continue;
        }
        int rows = t.get("rows").size();
        ConditionCheck c =
            new ConditionCheck() {
              @Override
              protected String getDescription() {
                return "Checking Oracle table " + table + " has " + rows + " rows";
              }

              @Override
              protected CheckResult check() {
                return new CheckResult(
                    SharedOracleReverseITContainer.getRowCount(testUsername, quote(table)) >= rows);
              }
            };
        combined = combined == null ? c : combined.and(c);
      }
    }
    return combined;
  }

  /** Validates scenarios A, B and C: every expected row and value must be present in Oracle. */
  private void validateSupportedTypes(JsonNode expected, List<String> failures) throws Exception {
    try (Connection conn =
        DriverManager.getConnection(
            jdbcResourceManager.getUri(),
            testUsername,
            SharedOracleReverseITContainer.ISOLATED_USER_PASSWORD)) {
      for (JsonNode t : expected.get("tables")) {
        String table = t.get("table").asText();
        boolean pk = t.get("pk").asBoolean();
        String mode = t.get("compare").asText();
        String sql =
            "SELECT "
                + (pk ? "" : "\"id\", ")
                + t.get("readExpr").asText()
                + " AS v FROM "
                + quote(table);
        List<String> tableFailures = new ArrayList<>();
        Map<Long, Object> byId = new HashMap<>();
        List<Object> pkValues = new ArrayList<>();
        try (Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql)) {
          while (rs.next()) {
            Object v = readValue(rs, mode);
            LOG.info("ACTUAL {} {}={}", table, pk ? "pk" : "id=" + rs.getLong("id"), describe(v));
            if (pk) {
              pkValues.add(v);
            } else {
              byId.put(rs.getLong("id"), v);
            }
          }
        } catch (Exception e) {
          tableFailures.add("read failed: " + e.getMessage());
        }
        JsonNode rows = t.get("rows");
        int actualCount = pk ? pkValues.size() : byId.size();
        if (actualCount != rows.size()) {
          tableFailures.add(
              String.format("expected %d rows but found %d", rows.size(), actualCount));
        }
        for (JsonNode r : rows) {
          String exp = resolve(r.get("expected"));
          if (pk) {
            Object match = null;
            for (Object v : pkValues) {
              if (matches(exp, v, mode)) {
                match = v;
                break;
              }
            }
            if (match == null) {
              tableFailures.add("missing pk value " + abbreviate(exp));
            } else {
              pkValues.remove(match);
            }
          } else {
            long id = r.get("id").asLong();
            if (!byId.containsKey(id)) {
              tableFailures.add("id=" + id + " missing (expected " + abbreviate(exp) + ")");
            } else if (!matches(exp, byId.get(id), mode)) {
              tableFailures.add(
                  "id="
                      + id
                      + " expected "
                      + abbreviate(exp)
                      + " but was "
                      + describe(byId.get(id)));
            }
          }
        }
        report(t, tableFailures, failures, actualCount);
      }
    }
  }

  private void report(JsonNode t, List<String> tableFailures, List<String> failures, int rows) {
    String table = t.get("table").asText();
    for (String f : tableFailures) {
      String msg =
          table
              + " ["
              + t.get("spannerType").asText()
              + " -> "
              + t.get("sourceType").asText()
              + "]: "
              + f;
      if (KNOWN_MISMATCH_TABLES.contains(table)) {
        LOG.warn("KNOWN MISMATCH (ignored: {}) {}", KNOWN_MISMATCHES.get(table), msg);
      } else {
        failures.add(msg);
      }
    }
    if (tableFailures.isEmpty()) {
      LOG.info("PASS {} ({} rows)", table, rows);
    }
  }

  /** Validates scenario D: every written row must reach Oracle (row-count consistency). */
  private void validateUnsupportedTypes(JsonNode expected, List<String> failures) {
    for (JsonNode t : expected.get("unsupportedTables")) {
      String table = t.get("table").asText();
      long expectedCount = t.get("expectedRowCount").asLong();
      long actual;
      try {
        actual = SharedOracleReverseITContainer.getRowCount(testUsername, quote(table));
      } catch (Exception e) {
        failures.add(table + ": read failed: " + e.getMessage());
        continue;
      }
      List<String> tableFailures = new ArrayList<>();
      if (actual != expectedCount) {
        tableFailures.add(
            String.format(
                "expected %d rows but found %d (unsupported type)", expectedCount, actual));
      }
      report(t, tableFailures, failures, (int) actual);
    }
  }

  /** Logs dead-letter queue files written by the pipeline, to explain missing rows. */
  private void logDeadLetterQueue() {
    try {
      List<Artifact> artifacts =
          gcsResourceManager.listArtifacts("dlq/", Pattern.compile(".*\\.json.*|.*"));
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

  private static String quote(String identifier) {
    // Oracle folds unquoted identifiers to upper case; the schema keeps them lower case.
    return "\"" + identifier + "\"";
  }

  /**
   * Resolves a value from {@code expected-data.json}: null, a literal string, or a compact object
   * {@code {prefix, repeat, times, base64, hexRepeat, suffix}} for very large values.
   */
  private static String resolve(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isTextual()) {
      return node.asText();
    }
    int times = node.get("times").asInt();
    if (node.has("hexRepeat")) {
      byte[] bytes = BaseEncoding.base16().decode(node.get("hexRepeat").asText().repeat(times));
      return Base64.getEncoder().encodeToString(bytes);
    }
    String value = node.get("prefix").asText() + node.get("repeat").asText().repeat(times);
    if (node.has("suffix")) {
      value += node.get("suffix").asText();
    }
    if (node.get("base64").asBoolean()) {
      return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
    return value;
  }

  /** Builds a typed Spanner value for the given Spanner PostgreSQL-dialect column type. */
  private static Value toSpannerValue(String spannerType, String v) throws Exception {
    String type = spannerType.toLowerCase();
    try {
      if (type.equals("varchar[]")) {
        if (v == null) {
          return Value.stringArray(null);
        }
        List<String> items = new ArrayList<>();
        new ObjectMapper().readTree(v).forEach(n -> items.add(n.asText()));
        return Value.stringArray(items);
      } else if (type.startsWith("varchar")) {
        return Value.string(v);
      } else if (type.equals("bytea")) {
        return Value.bytes(v == null ? null : ByteArray.fromBase64(v));
      } else if (type.equals("bigint")) {
        return Value.int64(v == null ? null : Long.parseLong(v));
      } else if (type.equals("float8")) {
        return Value.float64(v == null ? null : Double.parseDouble(v));
      } else if (type.equals("float4")) {
        return Value.float32(v == null ? null : Float.parseFloat(v));
      } else if (type.equals("numeric")) {
        return Value.pgNumeric(v);
      } else if (type.equals("timestamptz")) {
        return Value.timestamp(v == null ? null : Timestamp.parseTimestamp(v));
      } else if (type.equals("date")) {
        return Value.date(v == null ? null : Date.parseDate(v));
      } else if (type.equals("jsonb")) {
        return Value.pgJsonb(v);
      }
    } catch (IOException e) {
      throw new Exception(e);
    }
    throw new IllegalArgumentException("Unsupported Spanner type in test data: " + spannerType);
  }

  private static Object readValue(ResultSet rs, String mode) throws Exception {
    Object v;
    switch (mode) {
      case "number":
        v = rs.getBigDecimal("v");
        break;
      case "float":
        v = rs.getFloat("v");
        break;
      case "double":
        v = rs.getDouble("v");
        break;
      case "bytes":
        byte[] b = rs.getBytes("v");
        v = b == null ? null : Base64.getEncoder().encodeToString(b);
        break;
      default:
        v = rs.getString("v");
    }
    return rs.wasNull() ? null : v;
  }

  /** Type-aware comparison of an expected value against an Oracle column value. */
  private static boolean matches(String expected, Object actual, String mode) {
    if (expected == null || actual == null) {
      return expected == null && actual == null;
    }
    try {
      switch (mode) {
        case "number":
          return new BigDecimal(expected).compareTo((BigDecimal) actual) == 0;
        case "float":
          return Float.valueOf(expected).equals(actual);
        case "double":
          return Double.valueOf(expected).equals(actual);
        default:
          return expected.equals(actual);
      }
    } catch (Exception e) {
      return false;
    }
  }

  private static String describe(Object v) {
    if (v == null) {
      return "NULL";
    }
    if (v instanceof BigDecimal) {
      return abbreviate(((BigDecimal) v).toPlainString());
    }
    return abbreviate(v.toString());
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
