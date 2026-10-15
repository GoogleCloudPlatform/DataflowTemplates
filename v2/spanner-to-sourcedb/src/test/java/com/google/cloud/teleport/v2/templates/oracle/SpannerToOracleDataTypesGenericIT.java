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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
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
 * Generic data type integration test for the {@link SpannerToSourceDb} (reverse replication)
 * template with an Oracle target (scenario {@code reverse-datatypes-generic}).
 *
 * <p>Rows are inserted in Spanner into an all-types table and into two tables whose composite
 * primary keys together cover every key-capable Oracle data type in the mapping matrix; every
 * replicated Oracle value is asserted. The rows are then deleted in Spanner and the test checks
 * that the deletes (which address Oracle rows by every key type) are applied. Tables, Spanner
 * values and expected Oracle values are defined in {@code expected-data.json}.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToOracleDataTypesGenericIT extends SpannerToSourceDbITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToOracleDataTypesGenericIT.class);

  private static final String RESOURCE_DIR = "oracle/SpannerToOracleDataTypesGenericIT/";
  private static final String SPANNER_DDL_RESOURCE =
      RESOURCE_DIR + "oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";
  private static final String ORACLE_DDL_RESOURCE = RESOURCE_DIR + "oracle-schema.sql";
  private static final String EXPECTED_DATA_RESOURCE = RESOURCE_DIR + "expected-data.json";

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
    spannerResourceManager = setUpSpannerResourceManager();
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    spannerMetadataResourceManager = createSpannerMetadataDatabase();

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
            new HashMap<>());
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
   * SCOPE &amp; KNOWN LIMITATIONS (Spanner -&gt; Oracle, reverse replication)
   *
   * <p>Only default (matrix scenario A) mappings are used, so the mapping discrepancies documented
   * on {@code SpannerToOracleDataTypesIT#spannerToOracleDataTypes} do not apply here. Notes:
   *
   * <ul>
   *   <li><b>Excluded types</b>: types the reverse generator cannot write (INTERVAL, XMLTYPE, LONG,
   *       ROWID, ...) are excluded; BOOLEAN needs Oracle 23ai and JSON needs Oracle 21c+ (the
   *       shared test database is Oracle 19c); TIMESTAMP WITH TIME ZONE cannot be an Oracle primary
   *       key (ORA-02329) and BLOB, CLOB and NCLOB are not key-capable, so these are covered as
   *       non-key columns only.
   *   <li><b>Empty BYTES -&gt; Oracle RAW/BLOB is NULL</b>: Oracle treats a zero-length RAW (and
   *       {@code ''}) as <code>NULL</code>, so empty Spanner <code>BYTES</code> values are asserted
   *       as <code>NULL</code>.
   * </ul>
   */
  @Test
  public void spannerToOracleDataTypeConversionTest() throws Exception {
    assertThatPipeline(jobInfo).isRunning();
    JsonNode expected = loadExpectedData();

    // Insert rows in Spanner and wait until every row is in Oracle.
    writeRowsInSpanner(expected);
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)), () -> allRowsPresent(expected));
    assertThatResult(result).meetsConditions();

    // Assert every replicated value.
    List<String> failures = new ArrayList<>();
    validateRows(expected, failures);
    assertWithMessage("Data type validation failures:\n" + String.join("\n", failures))
        .that(failures)
        .isEmpty();

    // Delete the rows in Spanner (keys cover every key-capable type) and wait until they are gone.
    deleteRowsInSpanner(expected);
    PipelineOperator.Result resultDelete =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)), () -> allRowsDeleted(expected));
    assertThatResult(resultDelete).meetsConditions();
  }

  private JsonNode loadExpectedData() throws IOException {
    String json =
        Resources.toString(Resources.getResource(EXPECTED_DATA_RESOURCE), StandardCharsets.UTF_8);
    return new ObjectMapper().readTree(json);
  }

  private void writeRowsInSpanner(JsonNode expected) {
    for (JsonNode t : expected.get("tables")) {
      List<Mutation> mutations = new ArrayList<>();
      for (JsonNode row : t.get("rows")) {
        Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(t.get("table").asText());
        for (JsonNode c : t.get("columns")) {
          String name = c.get("name").asText();
          builder
              .set(name)
              .to(
                  toSpannerValue(
                      c.get("spannerType").asText(), resolve(row.get(name).get("spanner"))));
        }
        mutations.add(builder.build());
      }
      spannerResourceManager.write(mutations);
    }
    LOG.info("Inserted all rows in Spanner");
  }

  private void deleteRowsInSpanner(JsonNode expected) {
    List<Mutation> mutations = new ArrayList<>();
    for (JsonNode t : expected.get("tables")) {
      for (JsonNode row : t.get("rows")) {
        Key.Builder key = Key.newBuilder();
        for (JsonNode c : t.get("columns")) {
          if (c.get("pk").asBoolean()) {
            String name = c.get("name").asText();
            appendKeyPart(
                key, c.get("spannerType").asText(), resolve(row.get(name).get("spanner")));
          }
        }
        mutations.add(Mutation.delete(t.get("table").asText(), key.build()));
      }
    }
    spannerResourceManager.write(mutations);
    LOG.info("Deleted all rows in Spanner");
  }

  private boolean allRowsPresent(JsonNode expected) {
    for (JsonNode t : expected.get("tables")) {
      if (SharedOracleReverseITContainer.getRowCount(testUsername, quote(t.get("table").asText()))
          < t.get("rows").size()) {
        return false;
      }
    }
    return true;
  }

  private boolean allRowsDeleted(JsonNode expected) {
    boolean deleted = true;
    for (JsonNode t : expected.get("tables")) {
      long count =
          SharedOracleReverseITContainer.getRowCount(testUsername, quote(t.get("table").asText()));
      LOG.info("Rows remaining in {}: {}", t.get("table").asText(), count);
      deleted &= count == 0;
    }
    return deleted;
  }

  /**
   * Reads every row back from Oracle and compares every column. Rows are matched on {@code id}
   * (all-types table) or {@code val_col} (composite primary key tables).
   */
  private void validateRows(JsonNode expected, List<String> failures) throws Exception {
    try (Connection conn =
        DriverManager.getConnection(
            jdbcResourceManager.getUri(),
            testUsername,
            SharedOracleReverseITContainer.ISOLATED_USER_PASSWORD)) {
      for (JsonNode t : expected.get("tables")) {
        String table = t.get("table").asText();
        String matchColumn =
            t.get("columns").get(0).get("name").asText().equals("id") ? "id" : "val_col";
        List<String> select = new ArrayList<>();
        for (JsonNode c : t.get("columns")) {
          select.add(c.get("readExpr").asText() + " AS \"" + c.get("name").asText() + "\"");
        }
        Map<String, Map<String, Object>> actualRows = new HashMap<>();
        try (Statement st = conn.createStatement();
            ResultSet rs =
                st.executeQuery("SELECT " + String.join(", ", select) + " FROM " + quote(table))) {
          while (rs.next()) {
            Map<String, Object> values = new HashMap<>();
            for (JsonNode c : t.get("columns")) {
              String name = c.get("name").asText();
              values.put(name, readValue(rs, name, c.get("compare").asText()));
            }
            LOG.info("ACTUAL {} {}={}", table, matchColumn, values.get(matchColumn));
            actualRows.put(String.valueOf(values.get(matchColumn)), values);
          }
        }
        for (JsonNode row : t.get("rows")) {
          String key = resolve(row.get(matchColumn).get("expected"));
          Map<String, Object> actual = actualRows.get(key);
          if (actual == null) {
            failures.add(table + ": row " + key + " missing");
            continue;
          }
          for (JsonNode c : t.get("columns")) {
            String name = c.get("name").asText();
            String exp = resolve(row.get(name).get("expected"));
            if (!matches(exp, actual.get(name), c.get("compare").asText())) {
              failures.add(
                  String.format(
                      "%s row %s column %s [%s -> %s]: expected %s but was %s",
                      table,
                      key,
                      name,
                      c.get("spannerType").asText(),
                      c.get("sourceDdlType").asText(),
                      abbreviate(exp),
                      describe(actual.get(name))));
            }
          }
        }
        if (failures.stream().noneMatch(f -> f.startsWith(table))) {
          LOG.info("PASS {} ({} rows x {} columns)", table, actualRows.size(), select.size());
        }
      }
    }
  }

  private static String quote(String identifier) {
    // Oracle folds unquoted identifiers to upper case; the schema keeps them lower case.
    return "\"" + identifier + "\"";
  }

  /**
   * Resolves a value from {@code expected-data.json}: null, a literal string, or a compact object
   * {@code {prefix, repeat, times, base64, hexRepeat}} for very large values.
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
    if (node.get("base64").asBoolean()) {
      return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
    return value;
  }

  /** Builds a typed Spanner value for the given Spanner column type. */
  private static Value toSpannerValue(String spannerType, String v) {
    String type = spannerType.toUpperCase();
    if (type.startsWith("STRING")) {
      return Value.string(v);
    } else if (type.startsWith("BYTES")) {
      return Value.bytes(v == null ? null : ByteArray.fromBase64(v));
    } else if (type.equals("INT64")) {
      return Value.int64(v == null ? null : Long.parseLong(v));
    } else if (type.equals("FLOAT64")) {
      return Value.float64(v == null ? null : Double.parseDouble(v));
    } else if (type.equals("FLOAT32")) {
      return Value.float32(v == null ? null : Float.parseFloat(v));
    } else if (type.equals("NUMERIC")) {
      return Value.numeric(v == null ? null : new BigDecimal(v));
    } else if (type.equals("TIMESTAMP")) {
      return Value.timestamp(v == null ? null : Timestamp.parseTimestamp(v));
    } else if (type.equals("DATE")) {
      return Value.date(v == null ? null : Date.parseDate(v));
    } else if (type.equals("JSON")) {
      return Value.json(v);
    }
    throw new IllegalArgumentException("Unsupported Spanner type in test data: " + spannerType);
  }

  /** Appends a typed key part for deleting a row by its (composite) primary key. */
  private static void appendKeyPart(Key.Builder key, String spannerType, String v) {
    String type = spannerType.toUpperCase();
    if (type.startsWith("STRING")) {
      key.append(v);
    } else if (type.startsWith("BYTES")) {
      key.append(ByteArray.fromBase64(v));
    } else if (type.equals("INT64")) {
      key.append(Long.parseLong(v));
    } else if (type.equals("FLOAT64")) {
      key.append(Double.parseDouble(v));
    } else if (type.equals("FLOAT32")) {
      // Key.Builder has no float overload; widening to double is exact and Spanner converts the
      // key part back to the same FLOAT32 value.
      key.append((double) Float.parseFloat(v));
    } else if (type.equals("NUMERIC")) {
      key.append(new BigDecimal(v));
    } else if (type.equals("TIMESTAMP")) {
      key.append(Timestamp.parseTimestamp(v));
    } else if (type.equals("DATE")) {
      key.append(Date.parseDate(v));
    } else {
      throw new IllegalArgumentException("Unsupported key type in test data: " + spannerType);
    }
  }

  private static Object readValue(ResultSet rs, String column, String mode) throws Exception {
    Object v;
    switch (mode) {
      case "number":
        v = rs.getBigDecimal(column);
        break;
      case "float":
        v = rs.getFloat(column);
        break;
      case "double":
        v = rs.getDouble(column);
        break;
      case "bytes":
        byte[] b = rs.getBytes(column);
        v = b == null ? null : Base64.getEncoder().encodeToString(b);
        break;
      default:
        v = rs.getString(column);
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
    } catch (RuntimeException e) {
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
    if (s == null) {
      return "NULL";
    }
    String quoted = "'" + s + "'";
    return quoted.length() <= 120
        ? quoted
        : quoted.substring(0, 120) + "...(length=" + s.length() + ")";
  }
}
