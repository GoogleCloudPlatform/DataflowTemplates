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
public class JdbcToBigQueryTest {
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
      statement.execute("CREATE TABLE book (BOOK_ID int primary key, TITLE varchar(128))");
      statement.execute("INSERT INTO book VALUES (1, 'ABC')");
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
    options.setQuery("select * from book");
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
  public void testE2EWithDefaultApi() throws IOException, InterruptedException {
    JdbcToBigQuery.run(
            options, JdbcToBigQuery.writeToBQTransform(options).withTestServices(bigQueryServices))
        .waitUntilFinish();

    assertThat(fakeDatasetService.getAllRows(PROJECT, DATASET, TABLE))
        .isEqualTo(ImmutableList.of(new TableRow().set("BOOK_ID", 1).set("TITLE", "ABC")));
  }

  @Test
  public void testE2EWitStorageWriteApiAtLeastOnce() throws IOException, InterruptedException {
    options.setUseStorageWriteApi(true);
    options.setUseStorageWriteApiAtLeastOnce(true);

    JdbcToBigQuery.run(
            options, JdbcToBigQuery.writeToBQTransform(options).withTestServices(bigQueryServices))
        .waitUntilFinish();

    fakeDatasetService
        .getAllRows(PROJECT, DATASET, TABLE)
        .forEach(
            e -> assertThat(e).isEqualTo(new TableRow().set("book_id", "1").set("title", "ABC")));
  }

  @Test
  public void testE2EWitStorageWriteApi() throws IOException, InterruptedException {
    options.setUseStorageWriteApi(true);

    JdbcToBigQuery.run(
            options, JdbcToBigQuery.writeToBQTransform(options).withTestServices(bigQueryServices))
        .waitUntilFinish();

    assertThat(fakeDatasetService.getAllRows(PROJECT, DATASET, TABLE))
        .isEqualTo(ImmutableList.of(new TableRow().set("book_id", "1").set("title", "ABC")));
  }
}
