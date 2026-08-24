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
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.conditions.ChainedConditionCheck;
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
public class OracleSeparateShadowTableDatabaseShardedMigrationWithMigrationShardIdColumnIT extends DataStreamToSpannerITBase {
  private static final Logger LOG = LoggerFactory.getLogger(OracleSeparateShadowTableDatabaseShardedMigrationWithMigrationShardIdColumnIT.class);

  private static final String TABLE = "Users";
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleSeparateShadowTableDatabaseShardedMigrationWithMigrationShardIdColumnIT/oracle-google_standard_sql-spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "oracle/OracleSeparateShadowTableDatabaseShardedMigrationWithMigrationShardIdColumnIT/oracle-session.json";

  private static PipelineLauncher.LaunchInfo jobInfo;
  private static HashSet<OracleSeparateShadowTableDatabaseShardedMigrationWithMigrationShardIdColumnIT> testInstances = new HashSet<>();

  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager shadowSpannerResourceManager;
  public static GcsResourceManager gcsResourceManager;
  public static DatastreamResourceManager datastreamResourceManager;
  public static CloudOracleResourceManager cloudOracleSysUser;
  public static CloudOracleResourceManager jdbcResourceManagerShardA;
  private static String streamNameA;

  @Before
  public void setUp() throws Exception {
    skipBaseCleanup = true;
    synchronized (OracleSeparateShadowTableDatabaseShardedMigrationWithMigrationShardIdColumnIT.class) {
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
        builder.setSystemIdentifier("XE");
        cloudOracleSysUser = (CloudOracleResourceManager) builder.build();

        String oracleUser = "C##U" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
        String oraclePassword = "A" + RandomStringUtils.randomAlphanumeric(10);
        setUpOracleUser(oracleUser, oraclePassword);

        jdbcResourceManagerShardA = (CloudOracleResourceManager) CloudOracleResourceManager.builder(testName)
            .setUsername(oracleUser)
            .setPassword(oraclePassword)
            .setDatabaseName("XEPDB1")
            .setHost(System.getProperty("hostIp"))
            .setPort(1521)
            .build();

        executeSqlScript(jdbcResourceManagerShardA, "oracle/OracleSeparateShadowTableDatabaseShardedMigrationWithMigrationShardIdColumnIT/oracle-schema.sql");
        try { jdbcResourceManagerShardA.runSQLUpdate("ALTER TABLE \"Users\" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"); } catch (Exception e) {}

        OracleSource jdbcSource = OracleSource.builder(
            jdbcResourceManagerShardA.getHost(),
            jdbcResourceManagerShardA.getUsername(),
            jdbcResourceManagerShardA.getPassword(),
            jdbcResourceManagerShardA.getPort(),
            jdbcResourceManagerShardA.getDatabaseName())
            .setAllowedTables(Map.of(jdbcResourceManagerShardA.getUsername().toUpperCase(), List.of("Users")))
            .build();

        com.google.cloud.datastream.v1.SourceConfig sourceConfig = datastreamResourceManager.buildJDBCSourceConfig("jdbc-profile", jdbcSource);
        String uniqueCdcPrefix = "oracle-shard-cdc_" + org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric(5).toLowerCase() + "/cdc/";
        com.google.cloud.datastream.v1.DestinationConfig destinationConfig = datastreamResourceManager.buildGCSDestinationConfig(
                "gcs-profile", gcsResourceManager.getBucket(), uniqueCdcPrefix, DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT);
        com.google.cloud.datastream.v1.Stream stream = datastreamResourceManager.createStream(
                "test_stream_"+ RandomStringUtils.randomAlphanumeric(5).toLowerCase(), sourceConfig, destinationConfig);
        datastreamResourceManager.startStream(stream);
        streamNameA = stream.getName().substring(stream.getName().lastIndexOf('/') + 1);

        gcsResourceManager.createArtifact("input/shardingConfig.conf", generateSourceConfig(streamNameA, oracleUser, "L1"));
        
        Map<String, String> jobParams = new HashMap<>();
        jobParams.put("inputFileFormat", "avro");
        jobParams.put("inputFilePattern", "gs://" + gcsResourceManager.getBucket() + "/" + uniqueCdcPrefix);
        jobParams.put("datastreamSourceType", "oracle");
        jobParams.put("sourceConfigURL", getGcsPath("input/shardingConfig.conf", gcsResourceManager));
        jobParams.put("shadowTableSpannerInstanceId", shadowSpannerResourceManager.getInstanceId());
        jobParams.put("shadowTableSpannerDatabaseId", shadowSpannerResourceManager.getDatabaseId());

        if (System.getProperty("jdbcDriverJars") != null) {
            String driverPath = System.getProperty("jdbcDriverJars");
            jobParams.put("jdbcDriverJars", driverPath);
        }

        java.io.File fileLocal = new java.io.File("src/test/resources/" + SESSION_FILE_RESOURCE);
        if (!fileLocal.exists()) { fileLocal = new java.io.File("v2/datastream-to-spanner/src/test/resources/" + SESSION_FILE_RESOURCE); }
        String sessionFileContent = new String(java.nio.file.Files.readAllBytes(fileLocal.toPath()), java.nio.charset.StandardCharsets.UTF_8);
        sessionFileContent = sessionFileContent.replace("it_test", oracleUser).replace("shard_1", "L1");
        
        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName() + "shard1",
                null,
                null,
                "shard1",
                spannerResourceManager,
                null,
                jobParams,
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                sessionFileContent,
                null);
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
    for (OracleSeparateShadowTableDatabaseShardedMigrationWithMigrationShardIdColumnIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager, shadowSpannerResourceManager, pubsubResourceManager, gcsResourceManager, jdbcResourceManagerShardA, datastreamResourceManager, cloudOracleSysUser);
  }

  @Test
  public void multiShardMigration() throws Exception {
    assertThatPipeline(jobInfo).isRunning();

    ConditionCheck rowsConditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    new ConditionCheck() {
                        boolean executed = false;
                        @Override
                        protected String getDescription() { return "Insert Data into Oracle"; }
                        @Override
                        protected CheckResult check() {
                            if (!executed) {
                                insertDataInOracle();
                                executed = true;
                            }
                            return new CheckResult(true, "Inserted successfully");
                        }
                    },
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE)
                        .setMinRows(12)
                        .setMaxRows(12)
                        .build()
                )
        ).build();
            
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(45)), rowsConditionCheck);
            
    assertThatResult(result).meetsConditions();

    assertUsersTableContents();
  }

  private void insertDataInOracle() {
    LOG.info("Inserting rows into Users table in Oracle");
    jdbcResourceManagerShardA.runSQLUpdate("INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (1, 'Tester1', 20)");
    jdbcResourceManagerShardA.runSQLUpdate("INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (3, 'Tester3', 103)");
    jdbcResourceManagerShardA.runSQLUpdate("INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (13, 'Tester13', 113)");
    jdbcResourceManagerShardA.runSQLUpdate("INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (4, 'Tester4', 104)");
    jdbcResourceManagerShardA.runSQLUpdate("INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (5, 'Tester5', 105)");
    jdbcResourceManagerShardA.runSQLUpdate("INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (6, 'Tester6', 106)");
    jdbcResourceManagerShardA.runSQLUpdate("INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (7, 'Tester7', 107)");
    jdbcResourceManagerShardA.runSQLUpdate("INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (8, 'Tester8', 108)");
    jdbcResourceManagerShardA.runSQLUpdate("INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (9, 'Tester9', 109)");
    jdbcResourceManagerShardA.runSQLUpdate("INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (10, 'Tester10', 110)");
    jdbcResourceManagerShardA.runSQLUpdate("INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (11, 'Tester11', 111)");
    jdbcResourceManagerShardA.runSQLUpdate("INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (12, 'Tester12', 112)");
    jdbcResourceManagerShardA.runSQLUpdate("COMMIT");

    try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:oracle:thin:@" + System.getProperty("hostIp", "localhost") + ":1521/XEPDB1", "system", "TestPassword123");
         java.sql.Statement stmt = conn.createStatement()) {
        flushOracleRedoLogs(null);
    } catch (Exception e) {
        flushOracleRedoLogs(jdbcResourceManagerShardA);
    }
  }

  private void assertUsersTableContents() {
    List<Map<String, Object>> events = new ArrayList<>();
    events.add(Map.of("id", 1, "name", "Tester1", "age", 20, "migration_shard_id", "L1"));
    events.add(Map.of("id", 3, "name", "Tester3", "age", 103, "migration_shard_id", "L1"));
    events.add(Map.of("id", 13, "name", "Tester13", "age", 113, "migration_shard_id", "L1"));
    events.add(Map.of("id", 4, "name", "Tester4", "age", 104, "migration_shard_id", "L1"));
    events.add(Map.of("id", 5, "name", "Tester5", "age", 105, "migration_shard_id", "L1"));
    events.add(Map.of("id", 6, "name", "Tester6", "age", 106, "migration_shard_id", "L1"));
    events.add(Map.of("id", 7, "name", "Tester7", "age", 107, "migration_shard_id", "L1"));
    events.add(Map.of("id", 8, "name", "Tester8", "age", 108, "migration_shard_id", "L1"));
    events.add(Map.of("id", 9, "name", "Tester9", "age", 109, "migration_shard_id", "L1"));
    events.add(Map.of("id", 10, "name", "Tester10", "age", 110, "migration_shard_id", "L1"));
    events.add(Map.of("id", 11, "name", "Tester11", "age", 111, "migration_shard_id", "L1"));
    events.add(Map.of("id", 12, "name", "Tester12", "age", 112, "migration_shard_id", "L1"));

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from Users"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private String generateSourceConfig(
      String streamA, String dbA, String shardA) {
    return "{\n"
        + "  \"shardConfigs\": [\n"
        + "    {\n"
        + "      \"logicalShardId\": \"" + shardA + "\",\n"
        + "      \"dbName\": \"" + dbA + "\",\n"
        + "      \"streamId\": \"" + streamA + "\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }
}
