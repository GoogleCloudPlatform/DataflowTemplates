package com.google.cloud.teleport.v2.templates.oracle;

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

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class OracleSeparateShadowTableDatabaseStringOverridesIT extends DataStreamToSpannerITBase {

  private static final String ORACLE_TABLE = "person1";
  private static final String SPANNER_TABLE = "human1";

  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleSeparateShadowTableDatabaseStringOverridesIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";

  private static PipelineLauncher.LaunchInfo jobInfo;
  private static HashSet<OracleSeparateShadowTableDatabaseStringOverridesIT> testInstances = new HashSet<>();

  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager shadowSpannerResourceManager;
  public static GcsResourceManager gcsResourceManager;
  public static CloudOracleResourceManager cloudOracleSysUser;
  public static CloudOracleResourceManager cloudSqlResourceManager;
  public static DatastreamResourceManager datastreamResourceManager;

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (OracleSeparateShadowTableDatabaseStringOverridesIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        datastreamResourceManager = DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity(System.getProperty("privateConnectivity", "datastream-connect-2"))
                .build();
                
        spannerResourceManager = setUpSpannerResourceManager();
        shadowSpannerResourceManager = setUpShadowSpannerResourceManager();
        pubsubResourceManager = setUpPubSubResourceManager();
        gcsResourceManager = setUpSpannerITGcsResourceManager();
        
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

        org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.Builder builder = 
            org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.builder(testName);
        builder.setUsername("sys as sysdba");
        builder.setPassword(System.getProperty("cloudProxyPassword"));
        builder.setHost(System.getProperty("hostIp"));
        builder.setPort(1521);
        builder.setDatabaseName("XE");
        cloudOracleSysUser = (CloudOracleResourceManager) builder.build();

        cloudSqlResourceManager = (CloudOracleResourceManager) CloudOracleResourceManager.builder(testName)
            .setUsername(System.getProperty("cloudProxyUsername", "system"))
            .setPassword(System.getProperty("cloudProxyPassword", "TestPassword123"))
            .setDatabaseName("XE")
            .setHost(System.getProperty("hostIp"))
            .setPort(1521)
            .build();


        try { cloudSqlResourceManager.runSQLUpdate("DROP TABLE \"person1\""); } catch (Exception e) {}
        executeSqlScript(cloudSqlResourceManager, "oracle/OracleSeparateShadowTableDatabaseStringOverridesIT/oracle-schema.sql");

        OracleSource jdbcSource = OracleSource.builder(
            cloudSqlResourceManager.getHost(),
            cloudSqlResourceManager.getUsername(),
            cloudSqlResourceManager.getPassword(),
            cloudSqlResourceManager.getPort(),
            cloudSqlResourceManager.getDatabaseName())
            .setAllowedTables(Map.of(cloudSqlResourceManager.getUsername().toUpperCase(), List.of("person1")))
            .build();

        Map<String, String> overridesMap = new HashMap<>();
        overridesMap.put("inputFileFormat", "avro");
        overridesMap.put("datastreamSourceType", "oracle");
        overridesMap.put("tableOverrides", "[{person1, human1}]");
        overridesMap.put("columnOverrides", "[{person1.first_name1, person1.name1}]");
        overridesMap.put("shadowTableSpannerInstanceId", shadowSpannerResourceManager.getInstanceId());
        overridesMap.put("shadowTableSpannerDatabaseId", shadowSpannerResourceManager.getDatabaseId());

        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                null,
                null,
                "OracleSeparateShadowTableDatabaseStringOverridesIT",
                spannerResourceManager,
                pubsubResourceManager,
                overridesMap,
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                null,
                jdbcSource);
      }
    }
  }

  private void setUpOracleUser(String user, String password) {
    cloudOracleSysUser.runSQLUpdate(String.format("CREATE USER %s IDENTIFIED BY %s CONTAINER=ALL", user, password));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT EXECUTE_CATALOG_ROLE TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT CONNECT TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT CREATE SESSION TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.V_$DATABASE TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.V_$PDBS TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON DBA_SUPPLEMENTAL_LOGGING TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.V_$ARCHIVED_LOG TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.V_$LOGMNR_CONTENTS TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.V_$LOG TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.V_$LOGFILE TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.V_$THREAD TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.V_$PARAMETER TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.V_$NLS_PARAMETERS TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.V_$TIMEZONE_NAMES TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.V_$LOGMNR_LOGS TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.V_$ARCHIVE_DEST_STATUS TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.V_$TRANSACTION TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.DBA_REGISTRY TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.OBJ$ TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.ENC$ TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT CREATE TABLE TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT UNLIMITED TABLESPACE TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ANY DICTIONARY TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SET CONTAINER TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT LOGMINING TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT EXECUTE ON DBMS_LOGMNR TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT EXECUTE ON DBMS_LOGMNR_D TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ANY TRANSACTION TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ANY TABLE TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON DBA_EXTENTS TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT CREATE ANY TABLE TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("ALTER USER %s QUOTA 50m ON SYSTEM CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate(String.format("GRANT ALTER SYSTEM TO %s CONTAINER=ALL", user));
    cloudOracleSysUser.runSQLUpdate("ALTER DATABASE ADD SUPPLEMENTAL LOG DATA");

  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (OracleSeparateShadowTableDatabaseStringOverridesIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager, pubsubResourceManager, shadowSpannerResourceManager, gcsResourceManager, datastreamResourceManager, cloudOracleSysUser, cloudSqlResourceManager);
  }

  @Test
  public void migrationTestWithRenameTableAndColumns() {
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    new org.apache.beam.it.conditions.ConditionCheck() {
                      boolean executed = false;
                      @Override
                      protected String getDescription() {
                        return "Insert records into Oracle";
                      }
                      @Override
                      protected CheckResult check() {
                        if (!executed) {
                          cloudSqlResourceManager.runSQLUpdate("INSERT INTO \"person1\" (\"first_name1\", \"last_name1\") VALUES ('John', 'Doe')");
                          cloudSqlResourceManager.runSQLUpdate("INSERT INTO \"person1\" (\"first_name1\", \"last_name1\") VALUES ('Alice', 'Johnson')");
                          cloudSqlResourceManager.runSQLUpdate("COMMIT");
          flushOracleRedoLogs(cloudOracleSysUser);
                          executed = true;
                        }
                        return new CheckResult(true, "Inserted successfully");
                      }
                    },
                    SpannerRowsCheck.builder(spannerResourceManager, SPANNER_TABLE)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(45)),
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
