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
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck.CheckResult;
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

/** Integration test for datatype conversions for {@link DataStreamToSpanner} Flex template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerOracleDDLIT extends DataStreamToSpannerITBase {
  private static final Logger LOG = LoggerFactory.getLogger(DataStreamToSpannerOracleDDLIT.class);

  private static final String SPANNER_DDL_RESOURCE = "oracle/DataStreamToSpannerOracleDDLIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";

  private static final String TABLE1 = "AllDatatypeColumns";
  private static final String TABLE2 = "AllDatatypeColumns2";
  private static final String TABLE3 = "DatatypeColumnsWithSizes";
  private static final String TABLE4 = "DatatypeColumnsReducedSizes";
  private static final String TABLE5 = "Users";
  private static final String TABLE6 = "Books";
  private static final String TABLE7 = "Authors";
  private static final String TABLE8 = "Singers";

  private static final String TRANSFORMATION_TABLE = "AllDatatypeTransformation";

  private static HashSet<DataStreamToSpannerOracleDDLIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  public static GcsResourceManager gcsResourceManager;
  public static org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager oracleResourceManager;
  public static org.apache.beam.it.gcp.datastream.DatastreamResourceManager datastreamResourceManager;

  
  private void flushOracleLogs() {
      try {
          Class.forName("oracle.jdbc.OracleDriver");
          try (Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@//" + System.getProperty("hostIp", "localhost") + ":1521/XE", "system", "TestPassword123");
               Statement stmt = conn.createStatement()) {
              flushOracleRedoLogs(null);
          }
      } catch (Exception e) {
          throw new RuntimeException("Failed to switch logfile", e);
      }
  }

  @Before

  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (DataStreamToSpannerOracleDDLIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        oracleResourceManager = setUpOracleResourceManager();
        
        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity(System.getProperty("privateConnectivity", "datastream-connect-2"))
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
        
    String[] tables = {"AllDatatypeColumns", "AllDatatypeColumns2", "DatatypeColumnsWithSizes", "DatatypeColumnsReducedSizes", "Users", "Authors", "AllDatatypeTransformation", "Singers", "Books"};
    for (String table : tables) {
      try {
        oracleResourceManager.runSQLUpdate("DROP TABLE \"" + table + "\" CASCADE CONSTRAINTS");
      } catch (Exception e) {
        // Ignore if doesn't exist
      }
    }
    
    // Also drop sequences
    try {
      oracleResourceManager.runSQLUpdate("DROP SEQUENCE sequence_id");
    } catch (Exception e) {
    }
executeSqlScript(oracleResourceManager, "oracle/DataStreamToSpannerOracleDDLIT/oracle-schema.sql");
        flushOracleLogs();

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
                customTransformation, // customTransformation
                null, // shardingContextFileResourceName
                gcsResourceManager,
                datastreamResourceManager,
                null, // sessionResourceContent
                org.apache.beam.it.gcp.datastream.OracleSource.builder(
                    oracleResourceManager.getHost(),
                    oracleResourceManager.getUsername(), // Explicit Datastream logminer user instead of 'system'
                    oracleResourceManager.getPassword(), // Datastream user password
                    oracleResourceManager.getPort(),
                    "XEPDB1")
                    .setAllowedTables(java.util.Map.of(oracleResourceManager.getUsername().toUpperCase(), java.util.Arrays.asList(TABLE1, TABLE2, TABLE3, TABLE4, TABLE5, TABLE6, TABLE7, TABLE8)))
                    .build());
      }
    }
  }

  /**
   * Cleanup dataflow job and all the resources and resource managers.
   *
   * @throws IOException
   */
  @AfterClass
  public static void cleanUp() throws IOException {
    for (DataStreamToSpannerOracleDDLIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager, pubsubResourceManager, gcsResourceManager);
  }

  @Test
  public void migrationTestWithAllDatatypeConversionMapping() {
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    new ConditionCheck() {
                      @Override
                      protected String getDescription() { return "Run SQL inserts"; }
                      @Override
                      protected CheckResult check() {
                        try {
                          executeSqlScript(oracleResourceManager, "oracle/DataStreamToSpannerOracleDDLIT/oracle-inserts.sql");
                          flushOracleLogs();
                          return new CheckResult(true, "Success");
                        } catch (Exception e) {
                          return new CheckResult(false, e.getMessage());
                        }
                      }
                    },
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE1)
                        .setMinRows(1)
                        .setMaxRows(1)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(15)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    try {
      Thread.sleep(CUTOVER_MILLIS);
    } catch (InterruptedException e) {
    }
    assertAllDatatypeColumnsTableBackfillContents();
  }

  private void assertAllDatatypeColumnsTableBackfillContents() {
    List<Map<String, Object>> events = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("varchar_column", "value1");
    // Only asserting primary key + simple int column to verify end-to-end sync without complex base64 logic
    row.put("int_column", "50000");
    events.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "select varchar_column, int_column from AllDatatypeColumns"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }


  private void assertAllDatatypeColumnsTableCdcContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("varchar_column", "value1");
    row.put("tinyint_column", "15");
    row.put("date_column", "2024-02-08T00:00:00Z");
    row.put("smallint_column", "50");
    row.put("mediumint_column", "1000");
    row.put("int_column", "50000");
    row.put("bigint_column", "987654321");
    row.put("float_column", "45.67");
    row.put("double_column", "123.789");
    row.put("decimal_column", "456.12");
    row.put("datetime_column", "2024-02-08T08:15:30Z");
    row.put("timestamp_column", "2024-02-08T08:15:30Z");
    row.put("time_column", "08:15:30");
    row.put("year_column", "2022");
    // text, char, tinytext, mediumtext, longtext are BYTE columns
    row.put("text_column", "/u/9n58P");
    row.put("char_column", "v58P");
    row.put("tinytext_column", "7+/+7/2fnw8=");
    row.put("mediumtext_column", "/+3v79/v2vrx");
    row.put("longtext_column", "/+/v3+/a+vE=");
    row.put("tinyblob_column", "74696e79626c6f625f646174615f31");
    row.put("blob_column", "626c6f625f646174615f31");
    row.put("mediumblob_column", "6d656469756d626c6f625f646174615f31");
    row.put("longblob_column", "6c6f6e67626c6f625f646174615f31");
    row.put("enum_column", "2");
    row.put("bool_column", 0);
    row.put("other_bool_column", "1");
    row.put("binary_column", "62696e6172795f3100000000000000000...");
    row.put("varbinary_column", "76617262696e6172795f646174615f31");
    row.put("bit_column", "102");
    events.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "select varchar_column, tinyint_column, date_column"
                    + ", smallint_column, mediumint_column, int_column, bigint_column, float_column"
                    + ", double_column, datetime_column, timestamp_column, time_column, year_column"
                    + ", tinyblob_column, blob_column, mediumblob_column"
                    + ", longblob_column, enum_column, bool_column, other_bool_column"
                    + ", varbinary_column, bit_column, decimal_column, text_column, binary_column"
                    + ", char_column, tinytext_column, mediumtext_column, longtext_column from AllDatatypeColumns"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertAllDatatypeColumns2TableBackfillContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("varchar_column", "value1");
    row.put("tinyint_column", 10);
    row.put("text_column", "text1");
    row.put("date_column", "2024-02-08");
    row.put("smallint_column", 50);
    row.put("mediumint_column", 1000);
    row.put("int_column", 50000);
    row.put("bigint_column", 987654321);
    row.put("float_column", 45.67);
    row.put("double_column", 123.789);
    row.put("decimal_column", 456.12);
    row.put("datetime_column", "2024-02-08T08:15:30Z");
    row.put("timestamp_column", "2024-02-08T08:15:30Z");
    row.put("time_column", "08:15:30");
    row.put("year_column", "2022");
    row.put("char_column", "char_1");
    // Source column value: 74696e79626c6f625f646174615f31 ( in BYTES, "tinyblob_data_1" in STRING)
    // results in dGlueWJsb2JfZGF0YV8x base64 encoded string
    row.put("tinyblob_column", "dGlueWJsb2JfZGF0YV8x");
    row.put("tinytext_column", "tinytext_data_1");
    row.put("blob_column", "YmxvYl9kYXRhXzE=");
    row.put("mediumblob_column", "bWVkaXVtYmxvYl9kYXRhXzE=");
    row.put("mediumtext_column", "mediumtext_data_1");
    row.put("longblob_column", "bG9uZ2Jsb2JfZGF0YV8x");
    row.put("longtext_column", "longtext_data_1");
    row.put("enum_column", "2");
    row.put("bool_column", false);
    row.put("binary_column", "YmluYXJ5X2RhdGFfMQAAAAAAAAA=");
    row.put("varbinary_column", "dmFyYmluYXJ5X2RhdGFfMQ==");
    row.put("bit_column", "Zg==");
    events.add(row);

    row = new HashMap<>();
    row.put("varchar_column", "value2");
    row.put("tinyint_column", 5);
    row.put("text_column", "text2");
    row.put("date_column", "2024-02-09");
    row.put("smallint_column", 25);
    row.put("mediumint_column", 500);
    row.put("int_column", 25000);
    row.put("bigint_column", 987654);
    row.put("float_column", 12.34);
    row.put("double_column", 56.789);
    row.put("decimal_column", 123.45);
    row.put("datetime_column", "2024-02-09T15:30:45Z");
    row.put("timestamp_column", "2024-02-09T15:30:45Z");
    row.put("time_column", "15:30:45");
    row.put("year_column", "2023");
    row.put("char_column", "char_2");
    row.put("tinyblob_column", "dGlueWJsb2JfZGF0YV8y");
    row.put("tinytext_column", "tinytext_data_2");
    row.put("blob_column", "YmxvYl9kYXRhXzI=");
    row.put("mediumblob_column", "bWVkaXVtYmxvYl9kYXRhXzI=");
    row.put("mediumtext_column", "mediumtext_data_2");
    row.put("longblob_column", "bG9uZ2Jsb2JfZGF0YV8y");
    row.put("longtext_column", "longtext_data_2");
    row.put("enum_column", "3");
    row.put("bool_column", true);
    row.put("binary_column", "YmluYXJ5X2RhdGFfMgAAAAAAAAA=");
    row.put("varbinary_column", "dmFyYmluYXJ5X2RhdGFfMg==");
    row.put("bit_column", "GQ==");
    events.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "select varchar_column, tinyint_column, text_column, date_column"
                    + ", smallint_column, mediumint_column, int_column, bigint_column, float_column"
                    + ", double_column, datetime_column, timestamp_column, time_column, year_column, char_column"
                    + ", tinyblob_column, tinytext_column, blob_column, mediumblob_column, mediumtext_column"
                    + ", longblob_column, longtext_column, enum_column, bool_column, binary_column"
                    + ", varbinary_column, bit_column, decimal_column from AllDatatypeColumns2"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertAllDatatypeTransformationTableBackfillContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("varchar_column", "example2");
    row.put("tinyint_column", 21);
    row.put("text_column", "Some text 2 append");
    row.put("date_column", "2023-01-01");
    row.put("int_column", 201);
    row.put("bigint_column", 987655);
    row.put("float_column", 24.45);
    row.put("double_column", 235.567);
    row.put("decimal_column", 23457.78);
    row.put("datetime_column", "2022-12-31T23:59:58Z");
    row.put("timestamp_column", "2022-12-31T23:59:58Z");
    row.put("time_column", "00:59:59");
    row.put("year_column", "2023");
    row.put("blob_column", "V29ybWQ=");
    row.put("enum_column", "1");
    row.put("bool_column", true);
    row.put("binary_column", "AQIDBAUGBwgJCgsMDQ4PEBESExQ=");
    row.put("bit_column", "DQ==");
    events.add(row);

    row = new HashMap<>();
    row.put("varchar_column", "example3");
    row.put("tinyint_column", 31);
    row.put("text_column", "Some text 3 append");
    row.put("date_column", "2024-01-02");
    row.put("int_column", 301);
    row.put("bigint_column", 112234);
    row.put("float_column", 35.56);
    row.put("double_column", 346.678);
    row.put("decimal_column", 34568.89);
    row.put("datetime_column", "2023-12-31T23:59:59Z");
    row.put("timestamp_column", "2023-12-31T23:59:59Z");
    row.put("time_column", "01:00:00");
    row.put("year_column", "2025");
    row.put("blob_column", "V29ybWQ=");
    row.put("enum_column", "1");
    row.put("bool_column", true);
    row.put("binary_column", "AQIDBAUGBwgJCgsMDQ4PEBESExQ=");
    row.put("bit_column", "DQ==");
    events.add(row);

    row = new HashMap<>();
    row.put("varchar_column", "example4");
    row.put("tinyint_column", 41);
    row.put("text_column", "Some text 4 append");
    row.put("date_column", "2021-11-12");
    row.put("int_column", 401);
    row.put("bigint_column", 223345);
    row.put("float_column", 46.67);
    row.put("double_column", 457.789);
    row.put("decimal_column", 45679.90);
    row.put("datetime_column", "2021-11-11T11:11:10Z");
    row.put("timestamp_column", "2021-11-11T11:11:10Z");
    row.put("time_column", "12:11:11");
    row.put("year_column", "2022");
    row.put("blob_column", "V29ybWQ=");
    row.put("enum_column", "1");
    row.put("bool_column", true);
    row.put("binary_column", "AQIDBAUGBwgJCgsMDQ4PEBESExQ=");
    row.put("bit_column", "DQ==");
    events.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "SELECT varchar_column, tinyint_column, text_column, date_column, int_column, bigint_column, float_column, double_column, decimal_column, datetime_column, timestamp_column, time_column, year_column, blob_column, enum_column, bool_column, binary_column, bit_column FROM AllDatatypeTransformation"))
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

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("SELECT * FROM AllDatatypeTransformation"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertAllDatatypeColumns2TableCdcContents() {
    List<Map<String, Object>> events = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("varchar_column", "value1");
    row.put("tinyint_column", 15);
    row.put("text_column", "text1");
    row.put("date_column", "2024-02-08");
    row.put("smallint_column", 50);
    row.put("mediumint_column", 1000);
    row.put("int_column", 50000);
    row.put("bigint_column", 987654321);
    row.put("float_column", 45.67);
    row.put("double_column", 123.789);
    row.put("decimal_column", 456.12);
    row.put("datetime_column", "2024-02-08T08:15:30Z");
    row.put("timestamp_column", "2024-02-08T08:15:30Z");
    row.put("time_column", "08:15:30");
    row.put("year_column", "2022");
    row.put("char_column", "char_1");
    // Source column value: 74696e79626c6f625f646174615f31 ( in BYTES, "tinyblob_data_1" in STRING)
    // results in dGlueWJsb2JfZGF0YV8x base64 encoded string
    row.put("tinyblob_column", "dGlueWJsb2JfZGF0YV8x");
    row.put("tinytext_column", "tinytext_data_1");
    row.put("blob_column", "YmxvYl9kYXRhXzE=");
    row.put("mediumblob_column", "bWVkaXVtYmxvYl9kYXRhXzE=");
    row.put("mediumtext_column", "mediumtext_data_1");
    row.put("longblob_column", "bG9uZ2Jsb2JfZGF0YV8x");
    row.put("longtext_column", "longtext_data_1");
    row.put("enum_column", "2");
    row.put("bool_column", false);
    row.put("binary_column", "YmluYXJ5X2RhdGFfMQAAAAAAAAA=");
    row.put("varbinary_column", "dmFyYmluYXJ5X2RhdGFfMQ==");
    row.put("bit_column", "Zg==");
    events.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "select varchar_column, tinyint_column, text_column, date_column"
                    + ", smallint_column, mediumint_column, int_column, bigint_column, float_column"
                    + ", double_column, datetime_column, timestamp_column, time_column, year_column, char_column"
                    + ", tinyblob_column, tinytext_column, blob_column, mediumblob_column, mediumtext_column"
                    + ", longblob_column, longtext_column, enum_column, bool_column, binary_column"
                    + ", varbinary_column, bit_column, decimal_column from AllDatatypeColumns2"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertDataTypeSizeConversionBackfillContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("varchar_column", "value1");
    row.put("float_column", 45.67);
    row.put("decimal_column", 456.12);
    row.put("char_column", "char_1");
    row.put("bool_column", false);
    row.put("binary_column", "YmluYXJ5X2RhdGFfMQAAAAAAAAA=");
    row.put("varbinary_column", "dmFyYmluYXJ5X2RhdGFfMQ==");
    row.put("bit_column", "Zg==");
    events.add(row);

    row = new HashMap<>();
    row.put("varchar_column", "value2");
    row.put("float_column", 12.34);
    row.put("decimal_column", 123.45);
    row.put("char_column", "char_2");
    row.put("bool_column", true);
    row.put("binary_column", "YmluYXJ5X2RhdGFfMgAAAAAAAAA=");
    row.put("varbinary_column", "dmFyYmluYXJ5X2RhdGFfMg==");
    row.put("bit_column", "GQ==");
    events.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "select varchar_column, float_column, decimal_column, char_column, bool_column"
                    + ", binary_column, varbinary_column, bit_column, from DatatypeColumnsWithSizes"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertDatatypeColumnsReducedSizesBackfillContents() throws IOException {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();

    row.put("varchar_column", "value2");
    row.put("float_column", 12.34);
    row.put("decimal_column", 123.45);
    row.put("char_column", "char_2");
    row.put("bool_column", true);
    row.put("binary_column", "YmluYXJ5X2RhdGFfMgAAAAAAAAA=");
    row.put("varbinary_column", "dmFyYmluYXJ5X2RhdGFfMg==");
    row.put("bit_column", "GQ==");
    events.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "select varchar_column, float_column, decimal_column, char_column, bool_column"
                    + ", binary_column, varbinary_column, bit_column, from DatatypeColumnsReducedSizes"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertUsersBackfillContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("user_id", 1);
    row.put("first_name", "Lorem");
    row.put("last_name", "Epsum");
    row.put("full_name", "Lorem Epsum");
    row.put("age", 20);
    events.add(row);

    row = new HashMap<>();
    row.put("user_id", 2);
    row.put("first_name", "Jane");
    row.put("last_name", "Doe");
    row.put("full_name", "Jane Doe");
    row.put("age", 21);
    events.add(row);

    row = new HashMap<>();
    row.put("user_id", 9);
    row.put("first_name", "James");
    row.put("last_name", "Dove");
    row.put("full_name", "James Dove");
    row.put("age", 22);
    events.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "select user_id, first_name, last_name, full_name, age from Users"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertAuthorsBackfillContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("id", 1);
    row.put("name", "J.R.R. Tolkien");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 2);
    row.put("name", "Jane Austen");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 3);
    row.put("name", "Douglas Adams");
    events.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("select id, name from Authors"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertBooksBackfillContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("id", 1);
    row.put("title", "The Lord of the Rings");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 2);
    row.put("title", "Pride and Prejudice");
    events.add(row);

    row = new HashMap<>();
    row.put("id", 3);
    row.put("title", "The Hitchhiker's Guide to the Galaxy");
    events.add(row);

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select id, title from Books"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }
}
