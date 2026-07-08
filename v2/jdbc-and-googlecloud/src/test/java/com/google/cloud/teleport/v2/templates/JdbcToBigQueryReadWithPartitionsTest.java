/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.v2.options.JdbcToBigQueryOptions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link JdbcToBigQuery}. */
@RunWith(JUnit4.class)
public class JdbcToBigQueryReadWithPartitionsTest {
  private static final String PROJECT = "test-project";
  private static final String DATASET = "test-dataset";
  private static final String TABLE = "test-table";

  @Rule public final TemporaryFolder tmp = new TemporaryFolder();

  private FakeDatasetService fakeDatasetService;
  private BigQueryServices bigQueryServices;
  private JdbcToBigQueryOptions options;

  @Before
  public void setUp() throws SQLException, IOException, InterruptedException {
    // setup JDBC
    System.setProperty("derby.stream.error.field", "System.out"); // log to console, not a log file
    try (Connection conn = DriverManager.getConnection("jdbc:derby:memory:jdc2bq;create=true");
        Statement statement = conn.createStatement()) {
      statement.execute(
          "CREATE TABLE book (BOOK_ID bigint primary key, TITLE varchar(128), SELL_TIME timestamp)");
      statement.execute("INSERT INTO book VALUES (1, 'ABC', '2024-12-24 06:00:00.000')");
    }

    // setup BQ
    FakeDatasetService.setUp();
    fakeDatasetService = new FakeDatasetService();
    fakeDatasetService.createDataset(PROJECT, DATASET, "", "", null);
    TableSchema bqSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("BOOK_ID").setType("INTEGER"),
                    new TableFieldSchema().setName("TITLE").setType("STRING")));
    new TableFieldSchema().setName("SELL_TIME").setType("TIMESTAMP");
    fakeDatasetService.createTable(
        new Table()
            .setTableReference(
                new TableReference().setProjectId(PROJECT).setDatasetId(DATASET).setTableId(TABLE))
            .setSchema(bqSchema));
    FakeJobService fakeJobService = new FakeJobService();
    bigQueryServices =
        new FakeBigQueryServices()
            .withJobService(fakeJobService)
            .withDatasetService(fakeDatasetService);

    // setup options
    options = PipelineOptionsFactory.create().as(JdbcToBigQueryOptions.class);
    options.setDriverJars(tmp.newFile("empty-sql-driver.jar").getPath());
    options.setDriverClassName("org.apache.derby.jdbc.EmbeddedDriver");
    options.setConnectionURL("jdbc:derby:memory:jdc2bq");
    options.setConnectionProperties("");
    options.setTable("book");
    options.setOutputTable(PROJECT + ':' + DATASET + '.' + TABLE);
    options.setBigQueryLoadingTemporaryDirectory(tmp.newFolder("bq-tmp").getAbsolutePath());
  }

  @After
  public void tearDown() throws SQLException {
    try (Connection conn = DriverManager.getConnection("jdbc:derby:memory:jdc2bq");
        Statement statement = conn.createStatement()) {
      statement.execute("DROP TABLE book");
    }
  }

  @Test
  public void testE2EWithLongPartitionColumnType() throws IOException, InterruptedException {
    options.setPartitionColumn("BOOK_ID");
    options.setPartitionColumnType("long");

    JdbcToBigQuery.run(
            options, JdbcToBigQuery.writeToBQTransform(options).withTestServices(bigQueryServices))
        .waitUntilFinish();

    List<TableRow> rows = fakeDatasetService.getAllRows(PROJECT, DATASET, TABLE);
    // Filter out time to avoid timezone issues
    assertThat(rows.size()).isEqualTo(1);
    assertThat(rows.get(0).get("BOOK_ID")).isEqualTo(1);
    assertThat(rows.get(0).get("TITLE")).isEqualTo("ABC");
  }

  @Test
  public void testE2EWithDefaultPartitionColumnType() throws IOException, InterruptedException {
    options.setPartitionColumn("BOOK_ID");

    JdbcToBigQuery.run(
            options, JdbcToBigQuery.writeToBQTransform(options).withTestServices(bigQueryServices))
        .waitUntilFinish();

    List<TableRow> rows = fakeDatasetService.getAllRows(PROJECT, DATASET, TABLE);
    // Filter out time to avoid timezone issues
    assertThat(rows.size()).isEqualTo(1);
    assertThat(rows.get(0).get("BOOK_ID")).isEqualTo(1);
    assertThat(rows.get(0).get("TITLE")).isEqualTo("ABC");
  }

  @Test
  public void testE2EWithLongPartitionColumnTypeWithBounds()
      throws IOException, InterruptedException {
    options.setPartitionColumn("BOOK_ID");
    options.setPartitionColumnType("long");
    options.setLowerBound("1");
    options.setUpperBound("100");

    JdbcToBigQuery.run(
            options, JdbcToBigQuery.writeToBQTransform(options).withTestServices(bigQueryServices))
        .waitUntilFinish();

    List<TableRow> rows = fakeDatasetService.getAllRows(PROJECT, DATASET, TABLE);
    // Filter out time to avoid timezone issues
    assertThat(rows.size()).isEqualTo(1);
    assertThat(rows.get(0).get("BOOK_ID")).isEqualTo(1);
    assertThat(rows.get(0).get("TITLE")).isEqualTo("ABC");
  }

  @Test
  public void testE2EWithLongPartitionColumnTypeWithIncorrectBounds()
      throws IOException, InterruptedException {
    options.setPartitionColumn("BOOK_ID");
    options.setPartitionColumnType("long");
    options.setLowerBound("1");
    options.setUpperBound("2024-12-24");

    assertThrows(
        NumberFormatException.class,
        () ->
            JdbcToBigQuery.run(
                    options,
                    JdbcToBigQuery.writeToBQTransform(options).withTestServices(bigQueryServices))
                .waitUntilFinish());
  }

  @Test
  public void testE2EWithDateTimePartitionColumnType() throws IOException, InterruptedException {
    options.setPartitionColumn("SELL_TIME");
    options.setPartitionColumnType("datetime");

    JdbcToBigQuery.run(
            options, JdbcToBigQuery.writeToBQTransform(options).withTestServices(bigQueryServices))
        .waitUntilFinish();

    List<TableRow> rows = fakeDatasetService.getAllRows(PROJECT, DATASET, TABLE);
    // Filter out time to avoid timezone issues
    assertThat(rows.size()).isEqualTo(1);
    assertThat(rows.get(0).get("BOOK_ID")).isEqualTo(1);
    assertThat(rows.get(0).get("TITLE")).isEqualTo("ABC");
  }

  @Test
  public void testE2EWithDateTimePartitionColumnTypeWithBounds()
      throws IOException, InterruptedException {
    options.setPartitionColumn("SELL_TIME");
    options.setPartitionColumnType("datetime");
    options.setLowerBound("2024-12-23 06:00:00.000-08:00");
    options.setUpperBound("2024-12-24 07:00:00.000-08:00");

    JdbcToBigQuery.run(
            options, JdbcToBigQuery.writeToBQTransform(options).withTestServices(bigQueryServices))
        .waitUntilFinish();

    List<TableRow> rows = fakeDatasetService.getAllRows(PROJECT, DATASET, TABLE);
    // Filter out time to avoid timezone issues
    assertThat(rows.size()).isEqualTo(1);
    assertThat(rows.get(0).get("BOOK_ID")).isEqualTo(1);
    assertThat(rows.get(0).get("TITLE")).isEqualTo("ABC");
  }

  @Test
  public void testE2EWithDateTimePartitionColumnTypeWithIncorrectBounds()
      throws IOException, InterruptedException {
    options.setPartitionColumn("SELL_TIME");
    options.setPartitionColumnType("datetime");
    options.setLowerBound("1234");
    options.setUpperBound("2024-12-24 06:00:00");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            JdbcToBigQuery.run(
                    options,
                    JdbcToBigQuery.writeToBQTransform(options).withTestServices(bigQueryServices))
                .waitUntilFinish());
  }

  @Test
  public void testE2EWithIncorrectPartitionColumnType() throws IOException, InterruptedException {
    options.setPartitionColumn("BOOK_ID");
    options.setPartitionColumnType("dummytype");

    assertThrows(
        IllegalStateException.class,
        () ->
            JdbcToBigQuery.run(
                    options,
                    JdbcToBigQuery.writeToBQTransform(options).withTestServices(bigQueryServices))
                .waitUntilFinish());
  }
}
