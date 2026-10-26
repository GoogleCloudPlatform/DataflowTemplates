/*
 * Copyright (C) 2024 Google LLC
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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
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
 * Integration test for datatype conversions and DDL features for {@link DataStreamToSpanner} Flex
 * template with an Oracle source. Oracle version of {@code DataStreamToSpannerDDLIT}: the same
 * scenarios run against a live Datastream stream from the shared Oracle container instead of
 * pre-generated Datastream files. Events are produced with SQL ({@code
 * oracle/DataStreamToSpannerOracleDDLIT/*.sql}).
 *
 * <p>Oracle specific notes:
 *
 * <ul>
 *   <li>Datastream does not stream Oracle LOBs (CLOB, NCLOB, BLOB) unless large object streaming is
 *       enabled on the stream, which this test does not do: LOB columns are replicated as {@code
 *       NULL}.
 *   <li>Values of Oracle character columns mapped to Spanner {@code BYTES} are hex strings, as the
 *       template hex-decodes character data written to {@code BYTES} columns.
 *   <li>{@code AllDatatypeTransformation} uses {@code NUMBER(p <= 18)}, {@code BINARY_FLOAT} /
 *       {@code BINARY_DOUBLE}, {@code VARCHAR2} and {@code RAW} columns so that the change events
 *       carry the Java types {@code CustomTransformationWithShardForLiveIT} casts to: Datastream
 *       emits {@code NUMBER(p, 0)} as a long only up to 18 digits, and as a decimal string beyond.
 *   <li>Oracle has no per-table character set: {@link #migrationTestWithCharsetConversion()} covers
 *       the database character set ({@code VARCHAR2}) and the national character set ({@code
 *       NVARCHAR2}).
 * </ul>
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerOracleDDLIT extends DataStreamToSpannerITBase {
  private static final Logger LOG = LoggerFactory.getLogger(DataStreamToSpannerOracleDDLIT.class);

  private static final String RESOURCE_DIR = "oracle/DataStreamToSpannerOracleDDLIT/";
  private static final String ORACLE_DDL_RESOURCE = RESOURCE_DIR + "oracle-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      RESOURCE_DIR + "oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";

  private static final String TABLE1 = "AllDatatypeColumns";
  private static final String TABLE2 = "AllDatatypeColumns2";
  private static final String TABLE3 = "DatatypeColumnsWithSizes";
  private static final String TABLE4 = "DatatypeColumnsReducedSizes";
  private static final String TABLE5 = "Users";
  private static final String TABLE6 = "Books";
  private static final String TABLE7 = "Authors";
  private static final String TABLE8 = "Singers";

  private static final String TRANSFORMATION_TABLE = "AllDatatypeTransformation";

  private static final Duration CDC_WAIT = Duration.ofMinutes(8);

  private static HashSet<DataStreamToSpannerOracleDDLIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  public static GcsResourceManager gcsResourceManager;
  public static DatastreamResourceManager datastreamResourceManager;
  private static String oracleUser;

  /**
   * Setup resource managers and launch the Dataflow job (reading from a Datastream stream on all
   * the tables) once during the execution of this test class.
   */
  @Before
  public void setUp() throws Exception {
    skipBaseCleanup = true;
    synchronized (DataStreamToSpannerOracleDDLIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
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

  /** Creates the shared resources and launches the Dataflow job (sets {@link #jobInfo}). */
  private void setUpSharedResources() throws Exception {
    oracleUser = SharedOracleLiveITContainer.setupOracleIsolatedUser();

    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .setPrivateConnectivity(
                System.getProperty("privateConnectivity", "datastream-connect-2"))
            .build();

    spannerResourceManager = setUpSpannerResourceManager();
    pubsubResourceManager = setUpPubSubResourceManager();
    gcsResourceManager = setUpSpannerITGcsResourceManager();
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    createAndUploadJarToGcs("DatatypeIT", gcsResourceManager);
    CustomTransformation customTransformation =
        CustomTransformation.builder(
                "customTransformation.jar", "com.custom.CustomTransformationWithShardForLiveIT")
            .build();

    SharedOracleLiveITContainer.executeSqlScript(oracleUser, ORACLE_DDL_RESOURCE);
    SharedOracleLiveITContainer.flushRedoLogs();

    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            "DatatypeIT",
            spannerResourceManager,
            pubsubResourceManager,
            new HashMap<>() {
              {
                put("inputFileFormat", "avro");
              }
            },
            customTransformation,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            null,
            SharedOracleLiveITContainer.oracleSourceBuilder(oracleUser)
                .setAllowedTables(
                    Map.of(
                        oracleUser.toUpperCase(),
                        List.of(
                            TABLE1,
                            TABLE2,
                            TABLE3,
                            TABLE4,
                            TABLE5,
                            TABLE6,
                            TABLE7,
                            TABLE8,
                            TRANSFORMATION_TABLE)))
                .build());
  }

  /** Cleanup dataflow job and all the resources and resource managers. */
  @AfterClass
  public static void cleanUp() {
    cleanUpSharedResources(testInstances);
  }

  /**
   * Cleans up the Dataflow jobs of the given test instances, the shared resource managers and the
   * isolated Oracle user, then resets them so that a later setup (after a failed one) starts clean.
   */
  private static void cleanUpSharedResources(Collection<DataStreamToSpannerOracleDDLIT> instances) {
    try {
      SharedOracleLiveITContainer.cleanUpTestResources(
          instances,
          Arrays.asList(oracleUser),
          spannerResourceManager,
          pubsubResourceManager,
          gcsResourceManager,
          datastreamResourceManager);
    } finally {
      spannerResourceManager = null;
      pubsubResourceManager = null;
      gcsResourceManager = null;
      datastreamResourceManager = null;
      oracleUser = null;
    }
  }

  @Test
  public void migrationTestWithAllDatatypeConversionMapping() {
    runAndWait(TABLE1, "AllDatatypeColumns-backfill.sql", 2, startWait());
    assertAllDatatypeColumnsTableBackfillContents();

    runAndWait(TABLE1, "AllDatatypeColumns-cdc.sql", 1, CDC_WAIT);
    sleepForCutover();
    assertAllDatatypeColumnsTableCdcContents();
  }

  @Test
  public void migrationTestWithAllDatatypeDefaultMapping() {
    runAndWait(TABLE2, "AllDatatypeColumns2-backfill.sql", 2, startWait());
    assertAllDatatypeColumns2TableBackfillContents();

    runAndWait(TABLE2, "AllDatatypeColumns2-cdc.sql", 1, CDC_WAIT);
    sleepForCutover();
    assertAllDatatypeColumns2TableCdcContents();
  }

  @Test
  public void migrationTestWithAllDatatypeTransformation() {
    // The custom transformation filters out "example1" and transforms the inserted rows.
    runAndWait(TRANSFORMATION_TABLE, "AllDatatypeTransformation-backfill.sql", 3, startWait());
    assertAllDatatypeTransformationTableBackfillContents();

    // Updates are not transformed, "example4" is deleted.
    runAndWait(TRANSFORMATION_TABLE, "AllDatatypeTransformation-cdc.sql", 2, CDC_WAIT);
    sleepForCutover();
    assertAllDatatypeTransformationTableCdcContents();
  }

  @Test
  public void migrationTestWithDatatypeSizeConversion() {
    runAndWait(TABLE3, "DatatypeColumnsWithSizes.sql", 2, startWait());
    assertDataTypeSizeConversionBackfillContents();
  }

  @Test
  public void migrationTestWithDatatypeSizeReducedConversion() {
    // "value12345678901" fits in the Oracle VARCHAR2(20) column but not in the Spanner STRING(10)
    // column, so only "value2" is written to Spanner.
    runAndWait(TABLE4, "DatatypeColumnsReducedSizes.sql", 1, startWait());
    assertDatatypeColumnsReducedSizesBackfillContents();
  }

  @Test
  public void migrationTestWithGeneratedColumns() {
    runAndWait(TABLE5, "Users.sql", 3, startWait());
    assertUsersBackfillContents();
  }

  @Test
  public void migrationTestWithCharsetConversion() {
    runAndWait(TABLE7, "Authors.sql", 4, startWait());
    assertAuthorsBackfillContents();
  }

  @Test
  public void migrationTestWithSequenceColumns() {
    runAndWait(TABLE8, "Singers.sql", 2, startWait());
    assertSingersBackfillContents();
  }

  @Test
  public void migrationTestWithIdentityColumns() {
    runAndWait(TABLE6, "Books.sql", 3, startWait());
    assertBooksBackfillContents();
  }

  private static Duration startWait() {
    return Duration.ofMinutes(JOB_START_PROCESSING_WAIT_MINUTES);
  }

  /**
   * Runs the SQL statements of {@code sqlResource} in the isolated Oracle schema, then waits until
   * {@code table} has exactly {@code expectedRows} rows in Spanner.
   */
  private void runAndWait(String table, String sqlResource, int expectedRows, Duration timeout) {
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    executeOracleSql(sqlResource),
                    SpannerRowsCheck.builder(spannerResourceManager, table)
                        .setMinRows(expectedRows)
                        .setMaxRows(expectedRows)
                        .build()))
            .build();
    PipelineOperator.Result result =
        pipelineOperator().waitForCondition(createConfig(jobInfo, timeout), conditionCheck);
    assertThatResult(result).meetsConditions();
  }

  private ConditionCheck executeOracleSql(String sqlResource) {
    return new ConditionCheck() {
      private boolean executed = false;

      @Override
      protected String getDescription() {
        return "Execute Oracle SQL " + sqlResource;
      }

      @Override
      protected CheckResult check() {
        // Run the statements only once, even if the check is evaluated again.
        if (!executed) {
          try {
            SharedOracleLiveITContainer.executeSqlScript(oracleUser, RESOURCE_DIR + sqlResource);
            SharedOracleLiveITContainer.flushRedoLogs();
            executed = true;
          } catch (Exception e) {
            LOG.error("Failed to execute Oracle SQL {}", sqlResource, e);
            return new CheckResult(false, e.getMessage());
          }
        }
        return new CheckResult(true, "Executed " + sqlResource);
      }
    };
  }

  private static void sleepForCutover() {
    // Sleep for cutover time to wait till all CDCs propagate.
    // A real world customer also has a small cut over time to reach consistency.
    try {
      Thread.sleep(CUTOVER_MILLIS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Expected row of AllDatatypeColumns for "value1" / "value2" (conversion mapping). */
  private static Map<String, Object> allDatatypeColumnsRow(int n) {
    boolean first = n == 1;
    Map<String, Object> row = new HashMap<>();
    row.put("varchar_column", "value" + n);
    row.put("tinyint_column", first ? "10" : "5");
    row.put("date_column", first ? "2024-02-08T00:00:00Z" : "2024-02-09T00:00:00Z");
    row.put("smallint_column", first ? "50" : "25");
    row.put("mediumint_column", first ? "1000" : "500");
    row.put("int_column", first ? "50000" : "25000");
    row.put("bigint_column", first ? "987654321" : "987654");
    row.put("float_column", first ? "45.67" : "12.34");
    row.put("double_column", first ? "123.789" : "56.789");
    row.put("decimal_column", first ? "456.12" : "123.45");
    row.put("datetime_column", first ? "2024-02-08T08:15:30Z" : "2024-02-09T15:30:45Z");
    row.put("timestamp_column", first ? "2024-02-08T08:15:30Z" : "2024-02-09T15:30:45Z");
    row.put("time_column", first ? "08:15:30" : "15:30:45");
    row.put("year_column", first ? "2022" : "2023");
    // Character columns mapped to BYTES: hex strings decoded to bytes.
    row.put("char_column", first ? "Y2hhcjE=" : "Y2hhcjI=");
    row.put("tinytext_column", first ? "dGlueXRleHRfZGF0YV8xCg==" : "dGlueXRleHRfZGF0YV8yCg==");
    // RAW mapped to STRING: hex string.
    row.put(
        "tinyblob_column",
        first ? "74696E79626C6F625F646174615F31" : "74696E79626C6F625F646174615F32");
    // Spanner Value#toString truncates strings longer than 32 characters.
    row.put(
        "binary_column",
        first ? "62696E6172795F3100000000000000000..." : "62696E6172795F3200000000000000000...");
    row.put(
        "varbinary_column",
        first ? "76617262696E6172795F646174615F31" : "76617262696E6172795F646174615F32");
    row.put("bit_column", first ? "66" : "19");
    row.put("enum_column", first ? "2" : "3");
    row.put("bool_column", first ? 0 : 1);
    row.put("other_bool_column", first ? "1" : "0");
    // LOBs are not streamed by Datastream (Spanner NULL renders as "NULL").
    row.put("text_column", "NULL");
    row.put("blob_column", "NULL");
    row.put("mediumblob_column", "NULL");
    row.put("mediumtext_column", "NULL");
    row.put("longblob_column", "NULL");
    row.put("longtext_column", "NULL");
    return row;
  }

  private static final String ALL_DATATYPE_COLUMNS_QUERY =
      "SELECT varchar_column, tinyint_column, text_column, date_column, smallint_column,"
          + " mediumint_column, int_column, bigint_column, float_column, double_column,"
          + " decimal_column, datetime_column, timestamp_column, time_column, year_column,"
          + " char_column, tinyblob_column, tinytext_column, blob_column, mediumblob_column,"
          + " mediumtext_column, longblob_column, longtext_column, enum_column, bool_column,"
          + " other_bool_column, binary_column, varbinary_column, bit_column"
          + " FROM AllDatatypeColumns";

  private void assertAllDatatypeColumnsTableBackfillContents() {
    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery(ALL_DATATYPE_COLUMNS_QUERY))
        .hasRecordsUnorderedCaseInsensitiveColumns(
            List.of(allDatatypeColumnsRow(1), allDatatypeColumnsRow(2)));
  }

  private void assertAllDatatypeColumnsTableCdcContents() {
    Map<String, Object> row = allDatatypeColumnsRow(1);
    row.put("tinyint_column", "15");
    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery(ALL_DATATYPE_COLUMNS_QUERY))
        .hasRecordsUnorderedCaseInsensitiveColumns(List.of(row));
  }

  /** Expected row of AllDatatypeColumns2 for "value1" / "value2" (default mapping). */
  private static Map<String, Object> allDatatypeColumns2Row(int n) {
    boolean first = n == 1;
    Map<String, Object> row = new HashMap<>();
    row.put("varchar_column", "value" + n);
    row.put("tinyint_column", first ? 10 : 5);
    row.put("date_column", first ? "2024-02-08" : "2024-02-09");
    row.put("smallint_column", first ? 50 : 25);
    row.put("mediumint_column", first ? 1000 : 500);
    row.put("int_column", first ? 50000 : 25000);
    row.put("bigint_column", first ? 987654321 : 987654);
    row.put("float_column", first ? 45.67 : 12.34);
    row.put("double_column", first ? 123.789 : 56.789);
    row.put("decimal_column", first ? 456.12 : 123.45);
    row.put("datetime_column", first ? "2024-02-08T08:15:30Z" : "2024-02-09T15:30:45Z");
    row.put("timestamp_column", first ? "2024-02-08T08:15:30Z" : "2024-02-09T15:30:45Z");
    row.put("time_column", first ? "08:15:30" : "15:30:45");
    row.put("year_column", first ? "2022" : "2023");
    // Oracle CHAR(10) values are blank-padded.
    row.put("char_column", first ? "char_1    " : "char_2    ");
    row.put("tinyblob_column", first ? "dGlueWJsb2JfZGF0YV8x" : "dGlueWJsb2JfZGF0YV8y");
    row.put("tinytext_column", first ? "tinytext_data_1" : "tinytext_data_2");
    row.put("enum_column", first ? "2" : "3");
    row.put("bool_column", !first);
    row.put(
        "binary_column", first ? "YmluYXJ5X2RhdGFfMQAAAAAAAAA=" : "YmluYXJ5X2RhdGFfMgAAAAAAAAA=");
    row.put("varbinary_column", first ? "dmFyYmluYXJ5X2RhdGFfMQ==" : "dmFyYmluYXJ5X2RhdGFfMg==");
    row.put("bit_column", first ? "Zg==" : "GQ==");
    // LOBs are not streamed by Datastream (Spanner NULL renders as "NULL").
    row.put("text_column", "NULL");
    row.put("blob_column", "NULL");
    row.put("mediumblob_column", "NULL");
    row.put("mediumtext_column", "NULL");
    row.put("longblob_column", "NULL");
    row.put("longtext_column", "NULL");
    return row;
  }

  private static final String ALL_DATATYPE_COLUMNS2_QUERY =
      "SELECT varchar_column, tinyint_column, text_column, date_column, smallint_column,"
          + " mediumint_column, int_column, bigint_column, float_column, double_column,"
          + " decimal_column, datetime_column, timestamp_column, time_column, year_column,"
          + " char_column, tinyblob_column, tinytext_column, blob_column, mediumblob_column,"
          + " mediumtext_column, longblob_column, longtext_column, enum_column, bool_column,"
          + " binary_column, varbinary_column, bit_column FROM AllDatatypeColumns2";

  private void assertAllDatatypeColumns2TableBackfillContents() {
    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery(ALL_DATATYPE_COLUMNS2_QUERY))
        .hasRecordsUnorderedCaseInsensitiveColumns(
            List.of(allDatatypeColumns2Row(1), allDatatypeColumns2Row(2)));
  }

  private void assertAllDatatypeColumns2TableCdcContents() {
    Map<String, Object> row = allDatatypeColumns2Row(1);
    row.put("tinyint_column", 15);
    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery(ALL_DATATYPE_COLUMNS2_QUERY))
        .hasRecordsUnorderedCaseInsensitiveColumns(List.of(row));
  }

  private static final String TRANSFORMATION_QUERY =
      "SELECT varchar_column, tinyint_column, text_column, date_column, int_column,"
          + " bigint_column, float_column, double_column, decimal_column, datetime_column,"
          + " timestamp_column, time_column, year_column, blob_column, enum_column, bool_column,"
          + " binary_column, bit_column FROM AllDatatypeTransformation";

  /** Row as transformed by CustomTransformationWithShardForLiveIT for inserted rows. */
  private static Map<String, Object> transformedRow(
      String key,
      long tinyint,
      String text,
      String date,
      long intValue,
      long bigint,
      double floatValue,
      double doubleValue,
      double decimal,
      String datetime,
      String time,
      String year) {
    Map<String, Object> row = new HashMap<>();
    row.put("varchar_column", key);
    row.put("tinyint_column", tinyint);
    row.put("text_column", text);
    row.put("date_column", date);
    row.put("int_column", intValue);
    row.put("bigint_column", bigint);
    row.put("float_column", floatValue);
    row.put("double_column", doubleValue);
    row.put("decimal_column", decimal);
    row.put("datetime_column", datetime);
    row.put("timestamp_column", datetime);
    row.put("time_column", time);
    row.put("year_column", year);
    row.put("blob_column", "V29ybWQ=");
    row.put("enum_column", "1");
    row.put("bool_column", true);
    row.put("binary_column", "AQIDBAUGBwgJCgsMDQ4PEBESExQ=");
    row.put("bit_column", "DQ==");
    return row;
  }

  private void assertAllDatatypeTransformationTableBackfillContents() {
    List<Map<String, Object>> events = new ArrayList<>();
    events.add(
        transformedRow(
            "example2",
            21,
            "Some text 2 append",
            "2023-01-01",
            201,
            987655,
            24.45,
            235.567,
            23457.78,
            "2022-12-31T23:59:58Z",
            "00:59:59",
            "2023"));
    events.add(
        transformedRow(
            "example3",
            31,
            "Some text 3 append",
            "2024-01-02",
            301,
            112234,
            35.56,
            346.678,
            34568.89,
            "2023-12-31T23:59:59Z",
            "01:00:00",
            "2025"));
    events.add(
        transformedRow(
            "example4",
            41,
            "Some text 4 append",
            "2021-11-12",
            401,
            223345,
            46.67,
            457.789,
            45679.90,
            "2021-11-11T11:11:10Z",
            "12:11:11",
            "2022"));
    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery(TRANSFORMATION_QUERY))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertAllDatatypeTransformationTableCdcContents() {
    List<Map<String, Object>> events = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("varchar_column", "example2");
    row.put("tinyint_column", 25);
    row.put("text_column", "Updated text 2");
    row.put("date_column", "2023-01-01");
    row.put("int_column", 250);
    row.put("bigint_column", 56789);
    row.put("float_column", 25.45);
    row.put("double_column", 345.678);
    row.put("decimal_column", 23456.79);
    row.put("datetime_column", "2023-01-01T12:00:00Z");
    row.put("timestamp_column", "2023-01-01T12:00:00Z");
    row.put("time_column", "12:00:00");
    row.put("year_column", "2023");
    row.put("blob_column", "EjRWeJCrze8=");
    row.put("enum_column", "3");
    row.put("bool_column", true);
    row.put("binary_column", "EjRWeJCrze8SNFZ4kKvN7xI0Vng=");
    row.put("bit_column", "fw==");
    events.add(row);

    row = new HashMap<>();
    row.put("varchar_column", "example3");
    row.put("tinyint_column", 35);
    row.put("text_column", "Updated text 3");
    row.put("date_column", "2024-01-02");
    row.put("int_column", 350);
    row.put("bigint_column", 88000);
    row.put("float_column", 35.67);
    row.put("double_column", 456.789);
    row.put("decimal_column", 34567.90);
    row.put("datetime_column", "2024-01-02T00:00:00Z");
    row.put("timestamp_column", "2024-01-02T00:00:00Z");
    row.put("time_column", "01:00:00");
    row.put("year_column", "2025");
    row.put("blob_column", "q83vEjRWeJA=");
    row.put("enum_column", "1");
    row.put("bool_column", false);
    row.put("binary_column", "q83vEjRWeJCrze8SNFZ4kKvN7xI=");
    row.put("bit_column", "AA==");
    events.add(row);

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery(TRANSFORMATION_QUERY))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  /** Expected row of the DatatypeColumnsWithSizes / DatatypeColumnsReducedSizes tables. */
  private static Map<String, Object> sizesRow(String key, int n) {
    boolean first = n == 1;
    Map<String, Object> row = new HashMap<>();
    row.put("varchar_column", key);
    row.put("float_column", first ? 45.67 : 12.34);
    row.put("decimal_column", first ? 456.12 : 123.45);
    // Oracle CHAR(10) values are blank-padded.
    row.put("char_column", first ? "char_1    " : "char_2    ");
    row.put("bool_column", !first);
    row.put(
        "binary_column", first ? "YmluYXJ5X2RhdGFfMQAAAAAAAAA=" : "YmluYXJ5X2RhdGFfMgAAAAAAAAA=");
    row.put("varbinary_column", first ? "dmFyYmluYXJ5X2RhdGFfMQ==" : "dmFyYmluYXJ5X2RhdGFfMg==");
    row.put("bit_column", first ? "Zg==" : "GQ==");
    return row;
  }

  private void assertDataTypeSizeConversionBackfillContents() {
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "SELECT varchar_column, float_column, decimal_column, char_column, bool_column,"
                    + " binary_column, varbinary_column, bit_column FROM DatatypeColumnsWithSizes"))
        .hasRecordsUnorderedCaseInsensitiveColumns(
            List.of(sizesRow("value1", 1), sizesRow("value2", 2)));
  }

  private void assertDatatypeColumnsReducedSizesBackfillContents() {
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "SELECT varchar_column, float_column, decimal_column, char_column, bool_column,"
                    + " binary_column, varbinary_column, bit_column FROM"
                    + " DatatypeColumnsReducedSizes"))
        .hasRecordsUnorderedCaseInsensitiveColumns(List.of(sizesRow("value2", 2)));
  }

  private void assertUsersBackfillContents() {
    List<Map<String, Object>> events = new ArrayList<>();
    events.add(
        Map.of(
            "user_id",
            1,
            "first_name",
            "Lorem",
            "last_name",
            "Epsum",
            "full_name",
            "Lorem Epsum",
            "age",
            20));
    events.add(
        Map.of(
            "user_id",
            2,
            "first_name",
            "Jane",
            "last_name",
            "Doe",
            "full_name",
            "Jane Doe",
            "age",
            21));
    events.add(
        Map.of(
            "user_id",
            9,
            "first_name",
            "James",
            "last_name",
            "Dove",
            "full_name",
            "James Dove",
            "age",
            22));
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "SELECT user_id, first_name, last_name, full_name, age FROM Users"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertAuthorsBackfillContents() {
    List<Map<String, Object>> events = new ArrayList<>();
    events.add(authorRow(1, "J.R.R. Tolkien"));
    events.add(authorRow(2, "Gabriel García Márquez"));
    events.add(authorRow(3, "Фёдор Достоевский"));
    events.add(authorRow(4, "村上春樹"));
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("SELECT id, name, national_name FROM Authors"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  /** The same name is stored in VARCHAR2 (database charset) and NVARCHAR2 (national charset). */
  private static Map<String, Object> authorRow(int id, String name) {
    return Map.of("id", id, "name", name, "national_name", name);
  }

  private void assertSingersBackfillContents() {
    List<Map<String, Object>> events = new ArrayList<>();
    events.add(Map.of("singer_id", 1, "first_name", "Freddie", "last_name", "Mercury"));
    events.add(Map.of("singer_id", 2, "first_name", "Lata", "last_name", "Mangeshkar"));
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("SELECT singer_id, first_name, last_name FROM Singers"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertBooksBackfillContents() {
    List<Map<String, Object>> events = new ArrayList<>();
    events.add(Map.of("id", 1, "title", "The Lord of the Rings"));
    events.add(Map.of("id", 2, "title", "Pride and Prejudice"));
    events.add(Map.of("id", 3, "title", "The Hitchhiker's Guide to the Galaxy"));
    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("SELECT id, title FROM Books"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }
}
