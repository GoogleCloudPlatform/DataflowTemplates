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

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.OracleSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class OracleSeparateShadowTableDatabasePKFocusedIT extends DataStreamToSpannerITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(OracleSeparateShadowTableDatabasePKFocusedIT.class);

  private static final String ORACLE_SCHEMA =
      "oracle/OracleSeparateShadowTableDatabasePKFocusedIT/oracle-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleSeparateShadowTableDatabasePKFocusedIT/oracle-google_standard_sql-spanner-schema.sql";
  private static final String GCS_PATH_PREFIX = "OracleSeparateShadowTableDatabasePKFocusedIT";

  // Oracle defaults to uppercase unquoted identifiers
  private static final String ORACLE_TABLE_MY_TABLE = "MY_TABLE";
  private static final String ORACLE_TABLE_ALLTYPES = "ALLTYPES";
  private static final String SPANNER_TABLE_MY_TABLE = "MY_TABLE";
  private static final String SPANNER_TABLE_ALLTYPES = "ALLTYPES";

  private static CloudOracleResourceManager oracleSysUser;
  private static CloudOracleResourceManager oracleResourceManager;
  private static SpannerResourceManager shadowSpannerResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;
  private static boolean initialized = false;

  private static HashSet<OracleSeparateShadowTableDatabasePKFocusedIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  private void setUpOracleUser(String user, String password) {
    oracleSysUser.runSQLUpdate(
        String.format("CREATE USER %s IDENTIFIED BY %s CONTAINER=ALL", user, password));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT EXECUTE_CATALOG_ROLE TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT CONNECT TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT CREATE SESSION TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$DATABASE TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$PDBS TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON DBA_SUPPLEMENTAL_LOGGING TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$ARCHIVED_LOG TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$LOGMNR_CONTENTS TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$LOG TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$LOGFILE TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$THREAD TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$PARAMETER TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$NLS_PARAMETERS TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$TIMEZONE_NAMES TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$LOGMNR_LOGS TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$ARCHIVE_DEST_STATUS TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$TRANSACTION TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.DBA_REGISTRY TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.OBJ$ TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.ENC$ TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT CREATE TABLE TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT UNLIMITED TABLESPACE TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ANY DICTIONARY TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT SET CONTAINER TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT LOGMINING TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT EXECUTE ON DBMS_LOGMNR TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT EXECUTE ON DBMS_LOGMNR_D TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ANY TRANSACTION TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT SELECT ANY TABLE TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON DBA_EXTENTS TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT CREATE ANY TABLE TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("ALTER USER %s QUOTA 50m ON USERS CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT ALTER SYSTEM TO %s CONTAINER=ALL", user));
  }

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (OracleSeparateShadowTableDatabasePKFocusedIT.class) {
      testInstances.add(this);
      if (!initialized) {
        LOG.info("Setting up Oracle sys resource manager...");
        org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.Builder sysBuilder =
            org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.builder(testName);
        if (System.getProperty("cloudOracleHost") != null) {
          sysBuilder.setPassword(System.getProperty("cloudProxyPassword", "TestPassword123"));
          sysBuilder.setHost(System.getProperty("cloudOracleHost"));
          sysBuilder.setPort(1521);
          sysBuilder.setUsername("sys as sysdba");
          sysBuilder.setDatabaseName("XE");
        }
        oracleSysUser =
            (org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager)
                new SpannerOracleResourceManager(sysBuilder);

        String oracleUser = "C##U" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
        String oraclePassword = "A" + RandomStringUtils.randomAlphanumeric(10);

        LOG.info("Provisioning isolated user: " + oracleUser);
        setUpOracleUser(oracleUser, oraclePassword);

        org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.Builder builder =
            org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.builder(testName);
        if (System.getProperty("cloudOracleHost") != null) {
          builder.setPassword(oraclePassword);
          builder.setHost(System.getProperty("cloudOracleHost"));
          builder.setPort(1521);
          builder.setUsername(oracleUser);
          builder.setDatabaseName("XEPDB1");
        }
        oracleResourceManager =
            (org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager)
                new SpannerOracleResourceManager(builder);

        shadowSpannerResourceManager = setUpShadowSpannerResourceManager();
        spannerResourceManager = setUpSpannerResourceManager();
        gcsResourceManager = setUpSpannerITGcsResourceManager();
        pubsubResourceManager = setUpPubSubResourceManager();
        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity("datastream-connect-2")
                .build();

        executeSqlScript(oracleResourceManager, ORACLE_SCHEMA);
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

        OracleSource oracleSource =
            OracleSource.builder(
                    oracleResourceManager.getHost(),
                    oracleResourceManager.getUsername(),
                    oracleResourceManager.getPassword(),
                    oracleResourceManager.getPort(),
                    oracleResourceManager.getDatabaseName())
                .setAllowedTables(
                    Map.of(
                        oracleResourceManager.getUsername().toUpperCase(),
                        List.of(ORACLE_TABLE_MY_TABLE, ORACLE_TABLE_ALLTYPES)))
                .build();

        Map<String, String> overridesMap = new HashMap<>();
        overridesMap.put("inputFileFormat", "avro");
        overridesMap.put(
            "shadowTableSpannerInstanceId", shadowSpannerResourceManager.getInstanceId());
        overridesMap.put(
            "shadowTableSpannerDatabaseId", shadowSpannerResourceManager.getDatabaseId());

        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                null,
                null,
                GCS_PATH_PREFIX,
                spannerResourceManager,
                pubsubResourceManager,
                overridesMap,
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                null,
                oracleSource);

        assertThatPipeline(jobInfo).isRunning();
        initialized = true;
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (OracleSeparateShadowTableDatabasePKFocusedIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        oracleResourceManager,
        spannerResourceManager,
        shadowSpannerResourceManager,
        gcsResourceManager,
        pubsubResourceManager,
        datastreamResourceManager);
  }

  private void flushOracleLogs() {
    try (java.sql.Connection conn =
            java.sql.DriverManager.getConnection(
                "jdbc:oracle:thin:@" + System.getProperty("cloudOracleHost") + ":1521/XE",
                "system",
                System.getProperty("cloudProxyPassword", "TestPassword123"));
        java.sql.Statement stmt = conn.createStatement()) {
      flushOracleRedoLogs(null);
    } catch (Exception e) {
      LOG.warn("Log switch failed. Continuing...", e);
    }
  }

  @Test
  public void migrationTestSimpleTable() throws Exception {
    ConditionCheck insertRecords =
        new ConditionCheck() {
          boolean executed = false;

          @Override
          public String getDescription() {
            return "Insert 3 records into MY_TABLE";
          }

          @Override
          protected CheckResult check() {
            if (!executed) {
              try {
                oracleResourceManager.runSQLUpdate("INSERT INTO MY_TABLE (ID, VAL) VALUES (1, 10)");
                oracleResourceManager.runSQLUpdate("INSERT INTO MY_TABLE (ID, VAL) VALUES (2, 20)");
                oracleResourceManager.runSQLUpdate("INSERT INTO MY_TABLE (ID, VAL) VALUES (3, 30)");
                flushOracleLogs();
                executed = true;
              } catch (Exception e) {
                return new CheckResult(false, e.getMessage());
              }
            }
            return new CheckResult(true, "Data inserted");
          }
        };

    ConditionCheck updateDeleteRecords =
        new ConditionCheck() {
          boolean executed = false;

          @Override
          public String getDescription() {
            return "Update and Delete in MY_TABLE";
          }

          @Override
          protected CheckResult check() {
            if (!executed) {
              try {
                oracleResourceManager.runSQLUpdate("UPDATE MY_TABLE SET VAL = 10 WHERE ID = 2");
                oracleResourceManager.runSQLUpdate("DELETE FROM MY_TABLE WHERE ID = 1");
                flushOracleLogs();
                executed = true;
              } catch (Exception e) {
                return new CheckResult(false, e.getMessage());
              }
            }
            return new CheckResult(true, "Data updated/deleted");
          }
        };

    ConditionCheck pkUpdateRecords =
        new ConditionCheck() {
          boolean executed = false;

          @Override
          public String getDescription() {
            return "PK Update in MY_TABLE";
          }

          @Override
          protected CheckResult check() {
            if (!executed) {
              try {
                oracleResourceManager.runSQLUpdate("UPDATE MY_TABLE SET ID = 10 WHERE ID = 3");
                flushOracleLogs();
                executed = true;
              } catch (Exception e) {
                return new CheckResult(false, e.getMessage());
              }
            }
            return new CheckResult(true, "Data PK updated");
          }
        };

    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    insertRecords,
                    SpannerRowsCheck.builder(spannerResourceManager, SPANNER_TABLE_MY_TABLE)
                        .setMinRows(3)
                        .setMaxRows(3)
                        .build(),
                    updateDeleteRecords,
                    SpannerRowsCheck.builder(spannerResourceManager, SPANNER_TABLE_MY_TABLE)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build(),
                    pkUpdateRecords,
                    new ConditionCheck() {
                      @Override
                      public String getDescription() {
                        return "Check if MY_TABLE PK updated to 10";
                      }

                      @Override
                      protected CheckResult check() {
                        try {
                          com.google.common.collect.ImmutableList<com.google.cloud.spanner.Struct>
                              rows =
                                  spannerResourceManager.runQuery(
                                      "SELECT ID FROM MY_TABLE WHERE ID = 10");
                          if (!rows.isEmpty()) {
                            return new CheckResult(true, "PK updated to 10");
                          }
                        } catch (Exception e) {
                        }
                        return new CheckResult(false, "Not yet");
                      }
                    }))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(45)), conditionCheck);

    assertThatResult(result).meetsConditions();
    try {
      Thread.sleep(CUTOVER_MILLIS);
    } catch (Exception e) {
    }

    List<Map<String, Object>> events = new ArrayList<>();
    Map<String, Object> row1 = new HashMap<>();
    row1.put("ID", 2L);
    row1.put("VAL", 10L);
    Map<String, Object> row2 = new HashMap<>();
    row2.put("ID", 10L);
    row2.put("VAL", 30L);
    events.add(row1);
    events.add(row2);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("select ID, VAL from MY_TABLE"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  @Test
  public void migrationTestAllDataType() throws Exception {
    ConditionCheck insertRecords =
        new ConditionCheck() {
          boolean executed = false;

          @Override
          public String getDescription() {
            return "Insert records into ALLTYPES";
          }

          @Override
          protected CheckResult check() {
            if (!executed) {
              try {
                // 1
                oracleResourceManager.runSQLUpdate(
                    "INSERT INTO ALLTYPES (BOOL_FIELD, INT64_FIELD, FLOAT64_FIELD, STRING_FIELD,"
                        + " BYTES_FIELD, TIMESTAMP_FIELD, DATE_FIELD, NUMERIC_FIELD, VAL) VALUES"
                        + " ('true', 1, 3.14, 'This is a test string for MySQL.', '564768',"
                        + " TIMESTAMP '2024-12-20 10:30:00.00', TO_DATE('2024-12-20',"
                        + " 'YYYY-MM-DD'), 12345.1234, 10)");
                // 2
                oracleResourceManager.runSQLUpdate(
                    "INSERT INTO ALLTYPES (BOOL_FIELD, INT64_FIELD, FLOAT64_FIELD, STRING_FIELD,"
                        + " BYTES_FIELD, TIMESTAMP_FIELD, DATE_FIELD, NUMERIC_FIELD, VAL) VALUES"
                        + " ('true', 2, 3.1415, 'This is a test string for MySQL.', '564768',"
                        + " TIMESTAMP '2024-12-20 10:30:00.00', TO_DATE('2024-12-20',"
                        + " 'YYYY-MM-DD'), 12345.1234, 20)");
                // 3
                oracleResourceManager.runSQLUpdate(
                    "INSERT INTO ALLTYPES (BOOL_FIELD, INT64_FIELD, FLOAT64_FIELD, STRING_FIELD,"
                        + " BYTES_FIELD, TIMESTAMP_FIELD, DATE_FIELD, NUMERIC_FIELD, VAL) VALUES"
                        + " ('true', 3, 3.14159, 'This is a test string for MySQL.', '564768',"
                        + " TIMESTAMP '2024-12-20 10:30:00.00', TO_DATE('2024-12-20',"
                        + " 'YYYY-MM-DD'), 12345.1234, 30)");
                flushOracleLogs();
                executed = true;
              } catch (Exception e) {
                return new CheckResult(false, e.getMessage());
              }
            }
            return new CheckResult(true, "Data inserted");
          }
        };

    ConditionCheck updateDeleteRecords =
        new ConditionCheck() {
          boolean executed = false;

          @Override
          public String getDescription() {
            return "Update and Delete in ALLTYPES";
          }

          @Override
          protected CheckResult check() {
            if (!executed) {
              try {
                oracleResourceManager.runSQLUpdate("DELETE FROM ALLTYPES WHERE INT64_FIELD = 1");
                oracleResourceManager.runSQLUpdate(
                    "UPDATE ALLTYPES SET VAL = 10 WHERE INT64_FIELD = 2");
                flushOracleLogs();
                executed = true;
              } catch (Exception e) {
                return new CheckResult(false, e.getMessage());
              }
            }
            return new CheckResult(true, "Data updated/deleted");
          }
        };

    ConditionCheck pkUpdateRecords =
        new ConditionCheck() {
          boolean executed = false;

          @Override
          public String getDescription() {
            return "Update PK in ALLTYPES";
          }

          @Override
          protected CheckResult check() {
            if (!executed) {
              try {
                oracleResourceManager.runSQLUpdate(
                    "UPDATE ALLTYPES SET INT64_FIELD = 10 WHERE INT64_FIELD = 3");
                flushOracleLogs();
                executed = true;
              } catch (Exception e) {
                return new CheckResult(false, e.getMessage());
              }
            }
            return new CheckResult(true, "Data PK updated");
          }
        };

    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    insertRecords,
                    SpannerRowsCheck.builder(spannerResourceManager, SPANNER_TABLE_ALLTYPES)
                        .setMinRows(3)
                        .setMaxRows(3)
                        .build(),
                    updateDeleteRecords,
                    SpannerRowsCheck.builder(spannerResourceManager, SPANNER_TABLE_ALLTYPES)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build(),
                    pkUpdateRecords,
                    new ConditionCheck() {
                      @Override
                      public String getDescription() {
                        return "Check if ALLTYPES PK updated to 10";
                      }

                      @Override
                      protected CheckResult check() {
                        try {
                          com.google.common.collect.ImmutableList<com.google.cloud.spanner.Struct>
                              rows =
                                  spannerResourceManager.runQuery(
                                      "SELECT INT64_FIELD FROM ALLTYPES WHERE INT64_FIELD = 10");
                          if (!rows.isEmpty()) {
                            return new CheckResult(true, "PK updated to 10");
                          }
                        } catch (Exception e) {
                        }
                        return new CheckResult(false, "Not yet");
                      }
                    }))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(45)), conditionCheck);

    assertThatResult(result).meetsConditions();
    try {
      Thread.sleep(CUTOVER_MILLIS);
    } catch (Exception e) {
    }

    List<Map<String, Object>> events = new ArrayList<>();
    Map<String, Object> r1 = new HashMap<>();
    r1.put("INT64_FIELD", 2L);
    r1.put("VAL", 10L);
    Map<String, Object> r2 = new HashMap<>();
    r2.put("INT64_FIELD", 10L);
    r2.put("VAL", 30L);
    events.add(r1);
    events.add(r2);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("select INT64_FIELD, VAL from ALLTYPES"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }
}
