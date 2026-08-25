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
import org.testcontainers.shaded.com.google.common.io.Resources;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class OracleDataStreamToSpannerFileOverridesIT extends DataStreamToSpannerITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(OracleDataStreamToSpannerFileOverridesIT.class);

  private static final String ORACLE_SCHEMA =
      "oracle/OracleDataStreamToSpannerFileOverridesIT/oracle-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleDataStreamToSpannerFileOverridesIT/oracle-google_standard_sql-spanner-schema.sql";
  private static final String OVERRIDE_FILE =
      "oracle/OracleDataStreamToSpannerFileOverridesIT/override.json";
  private static final String GCS_PATH_PREFIX = "OracleFileOverridesIT";
  private static final String ORACLE_TABLE = "person1";
  private static final String SPANNER_TABLE = "human1";

  private static CloudOracleResourceManager oracleSysUser;
  private static CloudOracleResourceManager oracleResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;
  private static boolean initialized = false;

  private static HashSet<OracleDataStreamToSpannerFileOverridesIT> testInstances = new HashSet<>();

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
    synchronized (OracleDataStreamToSpannerFileOverridesIT.class) {
      testInstances.add(this);
      if (!initialized) {
        LOG.info("Setting up Oracle sys resource manager...");
        org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.Builder sysBuilder =
            org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.builder(testName);
        if (System.getProperty("hostIp") != null) {
          sysBuilder.setPassword(System.getProperty("cloudProxyPassword"));
          sysBuilder.setHost(System.getProperty("hostIp"));
          sysBuilder.setPort(1521);
          sysBuilder.setUsername("sys as sysdba");
          sysBuilder.setDatabaseName("XE");
        }
        oracleSysUser =
            (org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager) sysBuilder.build();

        String oracleUser = "C##U" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
        String oraclePassword = "A" + RandomStringUtils.randomAlphanumeric(10);

        LOG.info("Provisioning isolated user: " + oracleUser);
        setUpOracleUser(oracleUser, oraclePassword);

        org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.Builder builder =
            org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.builder(testName);
        if (System.getProperty("hostIp") != null) {
          builder.setPassword(oraclePassword);
          builder.setHost(System.getProperty("hostIp"));
          builder.setPort(1521);
          builder.setUsername(oracleUser);
          builder.setDatabaseName("/XEPDB1");
        }
        oracleResourceManager =
            (org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager) builder.build();

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

        gcsResourceManager.uploadArtifact(
            GCS_PATH_PREFIX + "/override.json", Resources.getResource(OVERRIDE_FILE).getPath());

        initialized = true;
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (OracleDataStreamToSpannerFileOverridesIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        oracleResourceManager,
        spannerResourceManager,
        gcsResourceManager,
        pubsubResourceManager,
        datastreamResourceManager);
  }

  @Test
  public void migrationTestWithRenameTableAndColumns() throws Exception {
    OracleSource oracleSource =
        OracleSource.builder(
                oracleResourceManager.getHost(),
                oracleResourceManager.getUsername(),
                oracleResourceManager.getPassword(),
                oracleResourceManager.getPort(),
                oracleResourceManager.getDatabaseName())
            .setAllowedTables(
                Map.of(oracleResourceManager.getUsername().toUpperCase(), List.of(ORACLE_TABLE)))
            .build();

    Map<String, String> overridesMap = new HashMap<>();
    overridesMap.put("inputFileFormat", "avro");
    overridesMap.put(
        "schemaOverridesFilePath",
        getGcsPath(GCS_PATH_PREFIX + "/override.json", gcsResourceManager));

    PipelineLauncher.LaunchInfo jobInfo =
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

    ConditionCheck sendDataCondition =
        new ConditionCheck() {
          @Override
          public String getDescription() {
            return "Insert data into Oracle and flush logs";
          }

          @Override
          protected CheckResult check() {
            try {
              oracleResourceManager.runSQLUpdate(
                  "INSERT INTO \"person1\" (\"ID\", \"first_name1\", \"last_name1\") VALUES (1, 'John', 'Doe')");
              oracleResourceManager.runSQLUpdate(
                  "INSERT INTO \"person1\" (\"ID\", \"first_name1\", \"last_name1\") VALUES (2, 'Alice', 'Johnson')");

              try (java.sql.Connection conn =
                      java.sql.DriverManager.getConnection(
                          "jdbc:oracle:thin:@"
                              + System.getProperty("hostIp", "localhost")
                              + ":1521/XEPDB1",
                          "system",
                          "TestPassword123");
                  java.sql.Statement stmt = conn.createStatement()) {
                flushOracleRedoLogs(null);
              }
              return new CheckResult(true, "Data inserted and logs flushed");
            } catch (Exception e) {
              LOG.error("Failed to insert data or flush logs", e);
              return new CheckResult(false, e.getMessage());
            }
          }
        };

    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    sendDataCondition,
                    SpannerRowsCheck.builder(spannerResourceManager, SPANNER_TABLE)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(JOB_START_PROCESSING_WAIT_MINUTES)),
                conditionCheck);

    assertThatResult(result).meetsConditions();
    assertHumanTableContents();
  }

  private void assertHumanTableContents() {
    List<Map<String, Object>> events = new ArrayList<>();
    Map<String, Object> row1 = new HashMap<>();
    row1.put("name1", "John");
    row1.put("last_name1", "Doe");
    Map<String, Object> row2 = new HashMap<>();
    row2.put("name1", "Alice");
    row2.put("last_name1", "Johnson");
    events.add(row1);
    events.add(row2);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("select name1, last_name1 from human1"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }
}
