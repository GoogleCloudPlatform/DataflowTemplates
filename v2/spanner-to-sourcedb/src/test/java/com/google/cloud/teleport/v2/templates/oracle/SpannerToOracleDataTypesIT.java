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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.OracleResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToOracleDataTypesIT extends SpannerToSourceDbITBase {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerToOracleDataTypesIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/SpannerToOracleDataTypesIT/oracle-googlesql-spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "oracle/SpannerToOracleDataTypesIT/session.json";
  private static final String ORACLE_SCHEMA_FILE_RESOURCE =
      "oracle/SpannerToOracleDataTypesIT/oracle-schema.sql";

  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager spannerMetadataResourceManager;
  public static OracleResourceManager jdbcResourceManager;
  public static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
    spannerMetadataResourceManager = createSpannerMetadataDatabase();
    jdbcResourceManager = SharedOracleReverseITContainer.getInstance();
    testUsername = setupOracleIsolatedUser(jdbcResourceManager);

    createOracleSchema(jdbcResourceManager, ORACLE_SCHEMA_FILE_RESOURCE, testUsername);

    gcsResourceManager = setUpSpannerITGcsResourceManager();

    // Setup shadow shard
    Map<String, org.apache.beam.it.jdbc.JDBCResourceManager> resources = new HashMap<>();
    resources.put("Shard1", jdbcResourceManager);
    createAndUploadShardConfigToGcs(gcsResourceManager, resources);

    try {
      gcsResourceManager.uploadArtifact(
          "input/session.json", Resources.getResource(SESSION_FILE_RESOURCE).getPath());
    } catch (Exception e) {
      gcsResourceManager.createArtifact("input/session.json", "{}");
    }

    pubsubResourceManager = setUpPubSubResourceManager();
    SubscriptionName subscriptionName =
        createPubsubResources(
            getClass().getSimpleName(),
            pubsubResourceManager,
            getGcsPath("dlq", gcsResourceManager)
                .replace("gs://" + gcsResourceManager.getBucket(), ""),
            gcsResourceManager);

    Map<String, String> jobParameters = new HashMap<>();

    String dlqGcsPubSubSubscription = subscriptionName.toString();

    jobInfo =
        launchDataflowJob(
            gcsResourceManager,
            spannerResourceManager,
            spannerMetadataResourceManager,
            dlqGcsPubSubSubscription,
            null,
            null,
            null,
            null,
            null,
            "oracle", // MUST NOT BE MYSQL_SOURCE_TYPE for oracle adapter
            jobParameters);
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void spannerToOracleDataTypes() {
    assertThatPipeline(jobInfo).isRunning();

    Map<String, List<Value>> spannerTableData = getSpannerTableData();
    writeRowsInSpanner(spannerTableData);

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                buildConditionCheck(spannerTableData));
    assertThatResult(result).meetsConditions();

    assertRowInOracle();
  }

  private void writeRowsInSpanner(Map<String, List<Value>> spannerTableData) {
    for (Map.Entry<String, List<Value>> tableDataEntry : spannerTableData.entrySet()) {
      String tableName = tableDataEntry.getKey();
      String[] parts = tableName.replace("_PK_TABLE", "").replace("_TABLE", "").split("_TO_");
      String baseType = parts.length > 1 ? parts[1] : parts[0];
      boolean isPk = tableName.endsWith("_PK_TABLE");
      String columnName = isPk ? baseType + "_PK_COL" : baseType + "_COL";
      // fix for nchar_varying stuff -> original base type doesn't have suffix explicitly unless
      // parsed right.
      // we can do simple thing:

      List<Value> vals = tableDataEntry.getValue();
      List<Mutation> mutations = new ArrayList<>(vals.size());
      for (int i = 0; i < vals.size(); i++) {
        Mutation m;
        // Don't insert NULL for PKs
        if (vals.get(i).isNull() && isPk) {
          continue;
        }

        if (isPk) {
          m =
              Mutation.newInsertOrUpdateBuilder(tableName)
                  .set(columnName)
                  .to(vals.get(i))
                  .set("DUMMY_COL")
                  .to("X")
                  .build();
        } else {
          m =
              Mutation.newInsertOrUpdateBuilder(tableName)
                  .set("ID")
                  .to(i + 1)
                  .set(columnName)
                  .to(vals.get(i))
                  .build();
        }
        mutations.add(m);
      }
      try {
        spannerResourceManager.write(mutations);
      } catch (Exception e) {
        throw new RuntimeException("Failed to write mutations to table: " + tableName, e);
      }
    }
  }

  private ConditionCheck buildConditionCheck(Map<String, List<Value>> spannerTableData) {
    ConditionCheck combinedCondition = null;
    for (Map.Entry<String, List<Value>> entry : spannerTableData.entrySet()) {
      String tableName = entry.getKey();
      if (tableName.toLowerCase().contains("raw")
          || tableName.toLowerCase().contains("blob")
          || tableName.toLowerCase().contains("clob")) {
        continue;
      }
      boolean isPk = tableName.endsWith("_PK_TABLE");
      int numRows = entry.getValue().size();
      if (isPk) {
        int nulls = 0;
        for (Value v : entry.getValue()) {
          if (v.isNull()) {
            nulls++;
          }
        }
        numRows -= nulls;
      }
      int finalNumRows = numRows;

      ConditionCheck c =
          new ConditionCheck() {
            @Override
            public String getDescription() {
              return "Checking num rows in oracle for " + tableName;
            }

            @Override
            public CheckResult check() {
              return new CheckResult(
                  runIsolatedGetRowCount(jdbcResourceManager, testUsername, tableName)
                      >= finalNumRows);
            }
          };
      if (combinedCondition == null) {
        combinedCondition = c;
      } else {
        combinedCondition = combinedCondition.and(c);
      }
    }
    return combinedCondition;
  }

  private void assertRowInOracle() {
    Map<String, List<Map<String, Object>>> expectedData = getExpectedData();
    for (Map.Entry<String, List<Map<String, Object>>> expectedTableData : expectedData.entrySet()) {
      String tableName = expectedTableData.getKey();
      List<Map<String, Object>> rawRows =
          runIsolatedReadTable(jdbcResourceManager, testUsername, tableName);
      List<Map<String, Object>> rows = cleanValues(rawRows);

      for (Map<String, Object> row : rows) {
        for (Map.Entry<String, Object> e : row.entrySet()) {
          if (e.getValue() != null && e.getValue() instanceof String) {
            String s = ((String) e.getValue()).replaceAll("\\s+$", "");
            if (s.matches("^-?\\d+\\.0$")) {
              s = s.substring(0, s.length() - 2);
            }
            if (s.isEmpty()) {
              e.setValue("NULL");
            } else {
              e.setValue(s);
            }
          }
        }
      }

      List<Map<String, Object>> expe = cleanValues(expectedTableData.getValue());
      for (Map<String, Object> row : expe) {
        for (Map.Entry<String, Object> e : row.entrySet()) {
          if (e.getValue() != null && e.getValue() instanceof String) {
            String s = ((String) e.getValue()).replaceAll("\\s+$", "");
            if (s.matches("^-?\\d+\\.0$")) {
              s = s.substring(0, s.length() - 2);
            }
            if (s.isEmpty()) {
              e.setValue("NULL");
            } else {
              e.setValue(s);
            }
          }
        }
      }

      try {
        org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords(rows)
            .hasRecordsUnorderedCaseInsensitiveColumns(expe);
      } catch (AssertionError e) {
        LOG.error("Assertion failed for table: " + tableName, e);
        throw e;
      }
    }
  }

  private List<Map<String, Object>> createRows(String columnName, boolean isPk, Object... values) {
    List<Object> vals = Arrays.asList(values);
    List<Map<String, Object>> rows = new ArrayList<>(vals.size());
    for (int i = 0; i < vals.size(); i++) {
      if (vals.get(i) == null && isPk) {
        continue;
      }
      Map<String, Object> row = new HashMap<>();
      if (isPk) {
        row.put("DUMMY_COL", "X");
      } else {
        row.put("ID", BigDecimal.valueOf(i + 1));
      }
      row.put(columnName, vals.get(i));
      rows.add(row);
    }
    return rows;
  }

  private List<Map<String, Object>> cleanValues(List<Map<String, Object>> rows) {
    for (Map<String, Object> row : rows) {
      for (Map.Entry<String, Object> entry : row.entrySet()) {
        if (entry.getValue() == null) {
          entry.setValue("NULL");
        } else if (entry.getValue() instanceof byte[]) {
          entry.setValue(Base64.getEncoder().encodeToString((byte[]) entry.getValue()));
        } else if (entry.getValue() instanceof java.sql.Timestamp) {
          entry.setValue(entry.getValue().toString());
        } else if (entry.getValue() instanceof java.sql.Clob) {
          try {
            java.sql.Clob c = (java.sql.Clob) entry.getValue();
            entry.setValue(c.getSubString(1, (int) c.length()));
          } catch (Exception ex) {
            entry.setValue(entry.getValue().toString());
          }
        } else if (entry.getValue() instanceof java.lang.Number) {
          entry.setValue(
              new java.math.BigDecimal(entry.getValue().toString())
                  .stripTrailingZeros()
                  .toPlainString());
        } else {
          entry.setValue(entry.getValue().toString());
        }
      }
    }
    return rows;
  }

  private Map<String, List<Value>> getSpannerTableData() {
    Map<String, List<Value>> spMap = new HashMap<>();
    spMap.put(
        "STRING_TO_VARCHAR2_TABLE",
        Arrays.asList(
            Value.string(""), Value.string(" "), Value.string("A"), Value.string("DROP TABLE")));
    spMap.put(
        "STRING_TO_VARCHAR2_PK_TABLE",
        Arrays.asList(Value.string("A"), Value.string("B"), Value.string("C")));
    spMap.put(
        "STRING_TO_VARCHAR_TABLE",
        Arrays.asList(
            Value.string(""), Value.string(" "), Value.string("A"), Value.string("DROP TABLE")));
    spMap.put(
        "STRING_TO_VARCHAR_PK_TABLE",
        Arrays.asList(Value.string("A"), Value.string("B"), Value.string("C")));
    spMap.put(
        "STRING_TO_CHAR_TABLE",
        Arrays.asList(
            Value.string(""), Value.string(" "), Value.string("A"), Value.string("DROP TABLE")));
    spMap.put(
        "STRING_TO_CHAR_PK_TABLE",
        Arrays.asList(Value.string("A"), Value.string("B"), Value.string("C")));
    spMap.put(
        "STRING_TO_CHARACTER_TABLE",
        Arrays.asList(
            Value.string(""), Value.string(" "), Value.string("A"), Value.string("DROP TABLE")));
    spMap.put(
        "STRING_TO_CHARACTER_PK_TABLE",
        Arrays.asList(Value.string("A"), Value.string("B"), Value.string("C")));
    spMap.put(
        "STRING_TO_NCHAR_TABLE",
        Arrays.asList(
            Value.string(""), Value.string(" "), Value.string("A"), Value.string("DROP TABLE")));
    spMap.put(
        "STRING_TO_NCHAR_PK_TABLE",
        Arrays.asList(Value.string("A"), Value.string("B"), Value.string("C")));
    spMap.put(
        "STRING_TO_NCHAR_VARYING_TABLE",
        Arrays.asList(
            Value.string(""), Value.string(" "), Value.string("A"), Value.string("DROP TABLE")));
    spMap.put(
        "STRING_TO_NCHAR_VARYING_PK_TABLE",
        Arrays.asList(Value.string("A"), Value.string("B"), Value.string("C")));
    spMap.put(
        "STRING_TO_NATIONAL_CHARACTER_TABLE",
        Arrays.asList(
            Value.string(""), Value.string(" "), Value.string("A"), Value.string("DROP TABLE")));
    spMap.put(
        "STRING_TO_NATIONAL_CHARACTER_PK_TABLE",
        Arrays.asList(Value.string("A"), Value.string("B"), Value.string("C")));
    spMap.put(
        "STRING_TO_NATIONAL_CHAR_TABLE",
        Arrays.asList(
            Value.string(""), Value.string(" "), Value.string("A"), Value.string("DROP TABLE")));
    spMap.put(
        "STRING_TO_NATIONAL_CHAR_PK_TABLE",
        Arrays.asList(Value.string("A"), Value.string("B"), Value.string("C")));
    spMap.put(
        "STRING_TO_NATIONAL_CHARACTER_VARYING_TABLE",
        Arrays.asList(
            Value.string(""), Value.string(" "), Value.string("A"), Value.string("DROP TABLE")));
    spMap.put(
        "STRING_TO_NATIONAL_CHARACTER_VARYING_PK_TABLE",
        Arrays.asList(Value.string("A"), Value.string("B"), Value.string("C")));
    spMap.put(
        "STRING_TO_NATIONAL_CHAR_VARYING_TABLE",
        Arrays.asList(
            Value.string(""), Value.string(" "), Value.string("A"), Value.string("DROP TABLE")));
    spMap.put(
        "STRING_TO_NATIONAL_CHAR_VARYING_PK_TABLE",
        Arrays.asList(Value.string("A"), Value.string("B"), Value.string("C")));
    spMap.put(
        "NUMERIC_TO_NUMBER_TABLE",
        Arrays.asList(
            Value.numeric(new BigDecimal("1")),
            Value.numeric(new BigDecimal("0")),
            Value.numeric(new BigDecimal("-1")),
            Value.numeric(null)));
    spMap.put(
        "FLOAT64_TO_NUMBER_TABLE",
        Arrays.asList(
            Value.float64(1.0), Value.float64(0.0), Value.float64(-1.0), Value.float64(null)));
    spMap.put(
        "STRING_TO_NUMBER_TABLE",
        Arrays.asList(
            Value.string("1"), Value.string("0"), Value.string("-1"), Value.string(null)));
    spMap.put(
        "INT64_TO_NUMBER_TABLE",
        Arrays.asList(Value.int64(100L), Value.int64(-100L), Value.int64(0L), Value.int64(null)));
    spMap.put(
        "NUMERIC_TO_NUMERIC_TABLE",
        Arrays.asList(
            Value.numeric(new BigDecimal("1")),
            Value.numeric(new BigDecimal("0")),
            Value.numeric(new BigDecimal("-1")),
            Value.numeric(null)));
    spMap.put(
        "FLOAT64_TO_NUMERIC_TABLE",
        Arrays.asList(
            Value.float64(1.0), Value.float64(0.0), Value.float64(-1.0), Value.float64(null)));
    spMap.put(
        "STRING_TO_NUMERIC_TABLE",
        Arrays.asList(
            Value.string("1"), Value.string("0"), Value.string("-1"), Value.string(null)));
    spMap.put(
        "INT64_TO_NUMERIC_TABLE",
        Arrays.asList(Value.int64(100L), Value.int64(-100L), Value.int64(0L), Value.int64(null)));
    spMap.put(
        "NUMERIC_TO_DECIMAL_TABLE",
        Arrays.asList(
            Value.numeric(new BigDecimal("1")),
            Value.numeric(new BigDecimal("0")),
            Value.numeric(new BigDecimal("-1")),
            Value.numeric(null)));
    spMap.put(
        "FLOAT64_TO_DECIMAL_TABLE",
        Arrays.asList(
            Value.float64(1.0), Value.float64(0.0), Value.float64(-1.0), Value.float64(null)));
    spMap.put(
        "STRING_TO_DECIMAL_TABLE",
        Arrays.asList(
            Value.string("1"), Value.string("0"), Value.string("-1"), Value.string(null)));
    spMap.put(
        "INT64_TO_DECIMAL_TABLE",
        Arrays.asList(Value.int64(100L), Value.int64(-100L), Value.int64(0L), Value.int64(null)));
    spMap.put(
        "NUMERIC_TO_DEC_TABLE",
        Arrays.asList(
            Value.numeric(new BigDecimal("1")),
            Value.numeric(new BigDecimal("0")),
            Value.numeric(new BigDecimal("-1")),
            Value.numeric(null)));
    spMap.put(
        "FLOAT64_TO_DEC_TABLE",
        Arrays.asList(
            Value.float64(1.0), Value.float64(0.0), Value.float64(-1.0), Value.float64(null)));
    spMap.put(
        "STRING_TO_DEC_TABLE",
        Arrays.asList(
            Value.string("1"), Value.string("0"), Value.string("-1"), Value.string(null)));
    spMap.put(
        "INT64_TO_DEC_TABLE",
        Arrays.asList(Value.int64(100L), Value.int64(-100L), Value.int64(0L), Value.int64(null)));
    spMap.put(
        "NUMERIC_TO_FLOAT_TABLE",
        Arrays.asList(
            Value.numeric(new BigDecimal("1")),
            Value.numeric(new BigDecimal("0")),
            Value.numeric(new BigDecimal("-1")),
            Value.numeric(null)));
    spMap.put(
        "FLOAT64_TO_FLOAT_TABLE",
        Arrays.asList(
            Value.float64(1.0), Value.float64(0.0), Value.float64(-1.0), Value.float64(null)));
    spMap.put(
        "STRING_TO_FLOAT_TABLE",
        Arrays.asList(
            Value.string("1"), Value.string("0"), Value.string("-1"), Value.string(null)));
    spMap.put(
        "INT64_TO_FLOAT_TABLE",
        Arrays.asList(Value.int64(100L), Value.int64(-100L), Value.int64(0L), Value.int64(null)));
    spMap.put(
        "FLOAT64_TO_DOUBLE_PRECISION_TABLE",
        Arrays.asList(
            Value.float64(1.0), Value.float64(0.0), Value.float64(-1.0), Value.float64(null)));
    spMap.put(
        "NUMERIC_TO_DOUBLE_PRECISION_TABLE",
        Arrays.asList(
            Value.numeric(new BigDecimal("1")),
            Value.numeric(new BigDecimal("0")),
            Value.numeric(new BigDecimal("-1")),
            Value.numeric(null)));
    spMap.put(
        "STRING_TO_DOUBLE_PRECISION_TABLE",
        Arrays.asList(
            Value.string("1"), Value.string("0"), Value.string("-1"), Value.string(null)));
    spMap.put(
        "INT64_TO_DOUBLE_PRECISION_TABLE",
        Arrays.asList(Value.int64(100L), Value.int64(-100L), Value.int64(0L), Value.int64(null)));
    spMap.put(
        "FLOAT64_TO_REAL_TABLE",
        Arrays.asList(
            Value.float64(1.0), Value.float64(0.0), Value.float64(-1.0), Value.float64(null)));
    spMap.put(
        "STRING_TO_REAL_TABLE",
        Arrays.asList(
            Value.string("1"), Value.string("0"), Value.string("-1"), Value.string(null)));
    spMap.put(
        "NUMERIC_TO_REAL_TABLE",
        Arrays.asList(
            Value.numeric(new BigDecimal("1")),
            Value.numeric(new BigDecimal("0")),
            Value.numeric(new BigDecimal("-1")),
            Value.numeric(null)));
    spMap.put(
        "INT64_TO_REAL_TABLE",
        Arrays.asList(Value.int64(100L), Value.int64(-100L), Value.int64(0L), Value.int64(null)));
    spMap.put(
        "FLOAT32_TO_BINARY_FLOAT_TABLE",
        Arrays.asList(
            Value.float32(1.0f), Value.float32(0.0f), Value.float32(-1.0f), Value.float32(null)));
    spMap.put(
        "FLOAT64_TO_BINARY_FLOAT_TABLE",
        Arrays.asList(
            Value.float64(1.0), Value.float64(0.0), Value.float64(-1.0), Value.float64(null)));
    spMap.put(
        "STRING_TO_BINARY_FLOAT_TABLE",
        Arrays.asList(
            Value.string("1"), Value.string("0"), Value.string("-1"), Value.string(null)));
    spMap.put(
        "NUMERIC_TO_BINARY_FLOAT_TABLE",
        Arrays.asList(
            Value.numeric(new BigDecimal("1")),
            Value.numeric(new BigDecimal("0")),
            Value.numeric(new BigDecimal("-1")),
            Value.numeric(null)));
    spMap.put(
        "INT64_TO_BINARY_FLOAT_TABLE",
        Arrays.asList(Value.int64(100L), Value.int64(-100L), Value.int64(0L), Value.int64(null)));
    spMap.put(
        "FLOAT64_TO_BINARY_DOUBLE_TABLE",
        Arrays.asList(
            Value.float64(1.0), Value.float64(0.0), Value.float64(-1.0), Value.float64(null)));
    spMap.put(
        "STRING_TO_BINARY_DOUBLE_TABLE",
        Arrays.asList(
            Value.string("1"), Value.string("0"), Value.string("-1"), Value.string(null)));
    spMap.put(
        "NUMERIC_TO_BINARY_DOUBLE_TABLE",
        Arrays.asList(
            Value.numeric(new BigDecimal("1")),
            Value.numeric(new BigDecimal("0")),
            Value.numeric(new BigDecimal("-1")),
            Value.numeric(null)));
    spMap.put(
        "INT64_TO_BINARY_DOUBLE_TABLE",
        Arrays.asList(Value.int64(100L), Value.int64(-100L), Value.int64(0L), Value.int64(null)));
    spMap.put(
        "INT64_TO_INTEGER_TABLE",
        Arrays.asList(Value.int64(100L), Value.int64(-100L), Value.int64(0L), Value.int64(null)));
    spMap.put(
        "INT64_TO_INTEGER_PK_TABLE",
        Arrays.asList(Value.int64(100L), Value.int64(-100L), Value.int64(0L), Value.int64(null)));
    spMap.put(
        "NUMERIC_TO_INTEGER_TABLE",
        Arrays.asList(
            Value.numeric(new BigDecimal("1")),
            Value.numeric(new BigDecimal("0")),
            Value.numeric(new BigDecimal("-1")),
            Value.numeric(null)));
    spMap.put(
        "STRING_TO_INTEGER_TABLE",
        Arrays.asList(
            Value.string("1"), Value.string("0"), Value.string("-1"), Value.string(null)));
    spMap.put(
        "FLOAT64_TO_INTEGER_TABLE",
        Arrays.asList(
            Value.float64(1.0), Value.float64(0.0), Value.float64(-1.0), Value.float64(null)));
    spMap.put(
        "INT64_TO_INT_TABLE",
        Arrays.asList(Value.int64(100L), Value.int64(-100L), Value.int64(0L), Value.int64(null)));
    spMap.put(
        "INT64_TO_INT_PK_TABLE",
        Arrays.asList(Value.int64(100L), Value.int64(-100L), Value.int64(0L), Value.int64(null)));
    spMap.put(
        "NUMERIC_TO_INT_TABLE",
        Arrays.asList(
            Value.numeric(new BigDecimal("1")),
            Value.numeric(new BigDecimal("0")),
            Value.numeric(new BigDecimal("-1")),
            Value.numeric(null)));
    spMap.put(
        "STRING_TO_INT_TABLE",
        Arrays.asList(
            Value.string("1"), Value.string("0"), Value.string("-1"), Value.string(null)));
    spMap.put(
        "FLOAT64_TO_INT_TABLE",
        Arrays.asList(
            Value.float64(1.0), Value.float64(0.0), Value.float64(-1.0), Value.float64(null)));
    spMap.put(
        "INT64_TO_SMALLINT_TABLE",
        Arrays.asList(Value.int64(100L), Value.int64(-100L), Value.int64(0L), Value.int64(null)));
    spMap.put(
        "INT64_TO_SMALLINT_PK_TABLE",
        Arrays.asList(Value.int64(100L), Value.int64(-100L), Value.int64(0L), Value.int64(null)));
    spMap.put(
        "NUMERIC_TO_SMALLINT_TABLE",
        Arrays.asList(
            Value.numeric(new BigDecimal("1")),
            Value.numeric(new BigDecimal("0")),
            Value.numeric(new BigDecimal("-1")),
            Value.numeric(null)));
    spMap.put(
        "STRING_TO_SMALLINT_TABLE",
        Arrays.asList(
            Value.string("1"), Value.string("0"), Value.string("-1"), Value.string(null)));
    spMap.put(
        "FLOAT64_TO_SMALLINT_TABLE",
        Arrays.asList(
            Value.float64(1.0), Value.float64(0.0), Value.float64(-1.0), Value.float64(null)));
    spMap.put(
        "STRING_TO_CLOB_TABLE",
        Arrays.asList(
            Value.string(""), Value.string(" "), Value.string("A"), Value.string("DROP TABLE")));
    spMap.put(
        "STRING_TO_NCLOB_TABLE",
        Arrays.asList(
            Value.string(""), Value.string(" "), Value.string("A"), Value.string("DROP TABLE")));
    spMap.put(
        "BOOL_TO_BOOLEAN_TABLE",
        Arrays.asList(Value.bool(true), Value.bool(false), Value.bool(null)));
    spMap.put(
        "BOOL_TO_BOOLEAN_PK_TABLE",
        Arrays.asList(Value.bool(true), Value.bool(false), Value.bool(null)));
    spMap.put(
        "INT64_TO_BOOLEAN_TABLE",
        Arrays.asList(Value.int64(1L), Value.int64(0L), Value.int64(null)));
    spMap.put(
        "STRING_TO_BOOLEAN_TABLE",
        Arrays.asList(Value.string("1"), Value.string("0"), Value.string(null)));
    return spMap;
  }

  private Map<String, List<Map<String, Object>>> getExpectedData() {
    Map<String, List<Map<String, Object>>> orMap = new HashMap<>();
    {
      String col = "varchar2_col";
      boolean isPk = false;
      orMap.put("STRING_TO_VARCHAR2_TABLE", createRows(col, isPk, "", " ", "A", "DROP TABLE"));
    }
    {
      String col = "varchar2_pk_col";
      boolean isPk = true;
      orMap.put("STRING_TO_VARCHAR2_PK_TABLE", createRows(col, isPk, "A", "B", "C"));
    }
    {
      String col = "varchar_col";
      boolean isPk = false;
      orMap.put("STRING_TO_VARCHAR_TABLE", createRows(col, isPk, "", " ", "A", "DROP TABLE"));
    }
    {
      String col = "varchar_pk_col";
      boolean isPk = true;
      orMap.put("STRING_TO_VARCHAR_PK_TABLE", createRows(col, isPk, "A", "B", "C"));
    }
    {
      String col = "char_col";
      boolean isPk = false;
      orMap.put("STRING_TO_CHAR_TABLE", createRows(col, isPk, "", " ", "A", "DROP TABLE"));
    }
    {
      String col = "char_pk_col";
      boolean isPk = true;
      orMap.put("STRING_TO_CHAR_PK_TABLE", createRows(col, isPk, "A", "B", "C"));
    }
    {
      String col = "character_col";
      boolean isPk = false;
      orMap.put("STRING_TO_CHARACTER_TABLE", createRows(col, isPk, "", " ", "A", "DROP TABLE"));
    }
    {
      String col = "character_pk_col";
      boolean isPk = true;
      orMap.put("STRING_TO_CHARACTER_PK_TABLE", createRows(col, isPk, "A", "B", "C"));
    }
    {
      String col = "nchar_col";
      boolean isPk = false;
      orMap.put("STRING_TO_NCHAR_TABLE", createRows(col, isPk, "", " ", "A", "DROP TABLE"));
    }
    {
      String col = "nchar_pk_col";
      boolean isPk = true;
      orMap.put("STRING_TO_NCHAR_PK_TABLE", createRows(col, isPk, "A", "B", "C"));
    }
    {
      String col = "nchar_varying_col";
      boolean isPk = false;
      orMap.put("STRING_TO_NCHAR_VARYING_TABLE", createRows(col, isPk, "", " ", "A", "DROP TABLE"));
    }
    {
      String col = "nchar_varying_pk_col";
      boolean isPk = true;
      orMap.put("STRING_TO_NCHAR_VARYING_PK_TABLE", createRows(col, isPk, "A", "B", "C"));
    }
    {
      String col = "national_character_col";
      boolean isPk = false;
      orMap.put(
          "STRING_TO_NATIONAL_CHARACTER_TABLE", createRows(col, isPk, "", " ", "A", "DROP TABLE"));
    }
    {
      String col = "national_character_pk_col";
      boolean isPk = true;
      orMap.put("STRING_TO_NATIONAL_CHARACTER_PK_TABLE", createRows(col, isPk, "A", "B", "C"));
    }
    {
      String col = "national_char_col";
      boolean isPk = false;
      orMap.put("STRING_TO_NATIONAL_CHAR_TABLE", createRows(col, isPk, "", " ", "A", "DROP TABLE"));
    }
    {
      String col = "national_char_pk_col";
      boolean isPk = true;
      orMap.put("STRING_TO_NATIONAL_CHAR_PK_TABLE", createRows(col, isPk, "A", "B", "C"));
    }
    {
      String col = "national_character_varying_col";
      boolean isPk = false;
      orMap.put(
          "STRING_TO_NATIONAL_CHARACTER_VARYING_TABLE",
          createRows(col, isPk, "", " ", "A", "DROP TABLE"));
    }
    {
      String col = "national_character_varying_pk_col";
      boolean isPk = true;
      orMap.put(
          "STRING_TO_NATIONAL_CHARACTER_VARYING_PK_TABLE", createRows(col, isPk, "A", "B", "C"));
    }
    {
      String col = "national_char_varying_col";
      boolean isPk = false;
      orMap.put(
          "STRING_TO_NATIONAL_CHAR_VARYING_TABLE",
          createRows(col, isPk, "", " ", "A", "DROP TABLE"));
    }
    {
      String col = "national_char_varying_pk_col";
      boolean isPk = true;
      orMap.put("STRING_TO_NATIONAL_CHAR_VARYING_PK_TABLE", createRows(col, isPk, "A", "B", "C"));
    }
    {
      String col = "number_col";
      boolean isPk = false;
      orMap.put(
          "NUMERIC_TO_NUMBER_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "number_col";
      boolean isPk = false;
      orMap.put(
          "FLOAT64_TO_NUMBER_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "number_col";
      boolean isPk = false;
      orMap.put(
          "STRING_TO_NUMBER_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "number_col";
      boolean isPk = false;
      orMap.put(
          "INT64_TO_NUMBER_TABLE",
          createRows(
              col, isPk, new BigDecimal("100"), new BigDecimal("-100"), new BigDecimal("0"), null));
    }
    {
      String col = "numeric_col";
      boolean isPk = false;
      orMap.put(
          "NUMERIC_TO_NUMERIC_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "numeric_col";
      boolean isPk = false;
      orMap.put(
          "FLOAT64_TO_NUMERIC_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "numeric_col";
      boolean isPk = false;
      orMap.put(
          "STRING_TO_NUMERIC_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "numeric_col";
      boolean isPk = false;
      orMap.put(
          "INT64_TO_NUMERIC_TABLE",
          createRows(
              col, isPk, new BigDecimal("100"), new BigDecimal("-100"), new BigDecimal("0"), null));
    }
    {
      String col = "decimal_col";
      boolean isPk = false;
      orMap.put(
          "NUMERIC_TO_DECIMAL_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "decimal_col";
      boolean isPk = false;
      orMap.put(
          "FLOAT64_TO_DECIMAL_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "decimal_col";
      boolean isPk = false;
      orMap.put(
          "STRING_TO_DECIMAL_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "decimal_col";
      boolean isPk = false;
      orMap.put(
          "INT64_TO_DECIMAL_TABLE",
          createRows(
              col, isPk, new BigDecimal("100"), new BigDecimal("-100"), new BigDecimal("0"), null));
    }
    {
      String col = "dec_col";
      boolean isPk = false;
      orMap.put(
          "NUMERIC_TO_DEC_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "dec_col";
      boolean isPk = false;
      orMap.put(
          "FLOAT64_TO_DEC_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "dec_col";
      boolean isPk = false;
      orMap.put(
          "STRING_TO_DEC_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "dec_col";
      boolean isPk = false;
      orMap.put(
          "INT64_TO_DEC_TABLE",
          createRows(
              col, isPk, new BigDecimal("100"), new BigDecimal("-100"), new BigDecimal("0"), null));
    }
    {
      String col = "float_col";
      boolean isPk = false;
      orMap.put(
          "NUMERIC_TO_FLOAT_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "float_col";
      boolean isPk = false;
      orMap.put(
          "FLOAT64_TO_FLOAT_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "float_col";
      boolean isPk = false;
      orMap.put(
          "STRING_TO_FLOAT_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "float_col";
      boolean isPk = false;
      orMap.put(
          "INT64_TO_FLOAT_TABLE",
          createRows(
              col, isPk, new BigDecimal("100"), new BigDecimal("-100"), new BigDecimal("0"), null));
    }
    {
      String col = "double_precision_col";
      boolean isPk = false;
      orMap.put(
          "FLOAT64_TO_DOUBLE_PRECISION_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "double_precision_col";
      boolean isPk = false;
      orMap.put(
          "NUMERIC_TO_DOUBLE_PRECISION_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "double_precision_col";
      boolean isPk = false;
      orMap.put(
          "STRING_TO_DOUBLE_PRECISION_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "double_precision_col";
      boolean isPk = false;
      orMap.put(
          "INT64_TO_DOUBLE_PRECISION_TABLE",
          createRows(
              col, isPk, new BigDecimal("100"), new BigDecimal("-100"), new BigDecimal("0"), null));
    }
    {
      String col = "real_col";
      boolean isPk = false;
      orMap.put(
          "FLOAT64_TO_REAL_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "real_col";
      boolean isPk = false;
      orMap.put(
          "STRING_TO_REAL_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "real_col";
      boolean isPk = false;
      orMap.put(
          "NUMERIC_TO_REAL_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "real_col";
      boolean isPk = false;
      orMap.put(
          "INT64_TO_REAL_TABLE",
          createRows(
              col, isPk, new BigDecimal("100"), new BigDecimal("-100"), new BigDecimal("0"), null));
    }
    {
      String col = "binary_float_col";
      boolean isPk = false;
      orMap.put(
          "FLOAT32_TO_BINARY_FLOAT_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "binary_float_col";
      boolean isPk = false;
      orMap.put(
          "FLOAT64_TO_BINARY_FLOAT_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "binary_float_col";
      boolean isPk = false;
      orMap.put(
          "STRING_TO_BINARY_FLOAT_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "binary_float_col";
      boolean isPk = false;
      orMap.put(
          "NUMERIC_TO_BINARY_FLOAT_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "binary_float_col";
      boolean isPk = false;
      orMap.put(
          "INT64_TO_BINARY_FLOAT_TABLE",
          createRows(
              col, isPk, new BigDecimal("100"), new BigDecimal("-100"), new BigDecimal("0"), null));
    }
    {
      String col = "binary_double_col";
      boolean isPk = false;
      orMap.put(
          "FLOAT64_TO_BINARY_DOUBLE_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "binary_double_col";
      boolean isPk = false;
      orMap.put(
          "STRING_TO_BINARY_DOUBLE_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "binary_double_col";
      boolean isPk = false;
      orMap.put(
          "NUMERIC_TO_BINARY_DOUBLE_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "binary_double_col";
      boolean isPk = false;
      orMap.put(
          "INT64_TO_BINARY_DOUBLE_TABLE",
          createRows(
              col, isPk, new BigDecimal("100"), new BigDecimal("-100"), new BigDecimal("0"), null));
    }
    {
      String col = "integer_col";
      boolean isPk = false;
      orMap.put(
          "INT64_TO_INTEGER_TABLE",
          createRows(
              col, isPk, new BigDecimal("100"), new BigDecimal("-100"), new BigDecimal("0"), null));
    }
    {
      String col = "integer_pk_col";
      boolean isPk = true;
      orMap.put(
          "INT64_TO_INTEGER_PK_TABLE",
          createRows(
              col, isPk, new BigDecimal("100"), new BigDecimal("-100"), new BigDecimal("0"), null));
    }
    {
      String col = "integer_col";
      boolean isPk = false;
      orMap.put(
          "NUMERIC_TO_INTEGER_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "integer_col";
      boolean isPk = false;
      orMap.put(
          "STRING_TO_INTEGER_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "integer_col";
      boolean isPk = false;
      orMap.put(
          "FLOAT64_TO_INTEGER_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "int_col";
      boolean isPk = false;
      orMap.put(
          "INT64_TO_INT_TABLE",
          createRows(
              col, isPk, new BigDecimal("100"), new BigDecimal("-100"), new BigDecimal("0"), null));
    }
    {
      String col = "int_pk_col";
      boolean isPk = true;
      orMap.put(
          "INT64_TO_INT_PK_TABLE",
          createRows(
              col, isPk, new BigDecimal("100"), new BigDecimal("-100"), new BigDecimal("0"), null));
    }
    {
      String col = "int_col";
      boolean isPk = false;
      orMap.put(
          "NUMERIC_TO_INT_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "int_col";
      boolean isPk = false;
      orMap.put(
          "STRING_TO_INT_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "int_col";
      boolean isPk = false;
      orMap.put(
          "FLOAT64_TO_INT_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "smallint_col";
      boolean isPk = false;
      orMap.put(
          "INT64_TO_SMALLINT_TABLE",
          createRows(
              col, isPk, new BigDecimal("100"), new BigDecimal("-100"), new BigDecimal("0"), null));
    }
    {
      String col = "smallint_pk_col";
      boolean isPk = true;
      orMap.put(
          "INT64_TO_SMALLINT_PK_TABLE",
          createRows(
              col, isPk, new BigDecimal("100"), new BigDecimal("-100"), new BigDecimal("0"), null));
    }
    {
      String col = "smallint_col";
      boolean isPk = false;
      orMap.put(
          "NUMERIC_TO_SMALLINT_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "smallint_col";
      boolean isPk = false;
      orMap.put(
          "STRING_TO_SMALLINT_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "smallint_col";
      boolean isPk = false;
      orMap.put(
          "FLOAT64_TO_SMALLINT_TABLE",
          createRows(
              col, isPk, new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("-1"), null));
    }
    {
      String col = "clob_col";
      boolean isPk = false;
      orMap.put("STRING_TO_CLOB_TABLE", createRows(col, isPk, "", " ", "A", "DROP TABLE"));
    }
    {
      String col = "nclob_col";
      boolean isPk = false;
      orMap.put("STRING_TO_NCLOB_TABLE", createRows(col, isPk, "", " ", "A", "DROP TABLE"));
    }
    {
      String col = "boolean_col";
      boolean isPk = false;
      orMap.put(
          "BOOL_TO_BOOLEAN_TABLE",
          createRows(col, isPk, new BigDecimal("1"), new BigDecimal("0"), null));
    }
    {
      String col = "boolean_pk_col";
      boolean isPk = true;
      orMap.put(
          "BOOL_TO_BOOLEAN_PK_TABLE",
          createRows(col, isPk, new BigDecimal("1"), new BigDecimal("0"), null));
    }
    {
      String col = "boolean_col";
      boolean isPk = false;
      orMap.put(
          "INT64_TO_BOOLEAN_TABLE",
          createRows(col, isPk, new BigDecimal("1"), new BigDecimal("0"), null));
    }
    {
      String col = "boolean_col";
      boolean isPk = false;
      orMap.put(
          "STRING_TO_BOOLEAN_TABLE",
          createRows(col, isPk, new BigDecimal("1"), new BigDecimal("0"), null));
    }
    return orMap;
  }
}
