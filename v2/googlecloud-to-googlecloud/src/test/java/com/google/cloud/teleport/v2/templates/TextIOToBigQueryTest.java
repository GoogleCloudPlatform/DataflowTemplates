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
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.io.IOException;
import java.sql.SQLException;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link TextIOToBigQuery}. */
@RunWith(JUnit4.class)
public class TextIOToBigQueryTest {

  private static final String SCHEMA_PATH =
      Resources.getResource("TextIOToBigQueryTest/schema.json").getPath();
  private static final String INPUT_PATH =
      Resources.getResource("TextIOToBigQueryTest/input.txt").getPath();
  private static final String UDF_PATH =
      Resources.getResource("TextIOToBigQueryTest/udf.js").getPath();

  private static final String PROJECT = "test-project";
  private static final String DATASET = "test-dataset";
  private static final String TABLE = "test-table";

  @Rule public final TemporaryFolder tmp = new TemporaryFolder();

  private FakeDatasetService fakeDatasetService;
  private BigQueryServices bigQueryServices;
  private TextIOToBigQuery.Options options;

  @Before
  public void setUp() throws SQLException, IOException, InterruptedException {
    // setup BQ
    FakeDatasetService.setUp();
    fakeDatasetService = new FakeDatasetService();
    fakeDatasetService.createDataset(PROJECT, DATASET, "", "", null);
    TableSchema bqSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("BOOK_ID").setType("STRING"),
                    new TableFieldSchema().setName("TITLE").setType("STRING"),
                    new TableFieldSchema()
                        .setName("DETAILS")
                        .setType("RECORD")
                        .setMode("NULLABLE")
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("YEAR").setType("INTEGER"),
                                new TableFieldSchema().setName("SUMMARY").setType("STRING")))));
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
    options = PipelineOptionsFactory.create().as(TextIOToBigQuery.Options.class);
    options.setJSONPath(SCHEMA_PATH);
    options.setInputFilePattern(INPUT_PATH);
    options.setJavascriptTextTransformGcsPath(UDF_PATH);
    options.setJavascriptTextTransformFunctionName("identity");
    options.setJobName("test-job"); // otherwise, the job name can randomly change during execution
    options.setOutputTable(TABLE);
    options.setOutputTable(PROJECT + '.' + DATASET + '.' + TABLE);
    options.setBigQueryLoadingTemporaryDirectory(tmp.newFolder("bq-tmp").getAbsolutePath());
  }

  @Test
  public void testE2EWithDefaultApi() throws IOException, InterruptedException {
    TextIOToBigQuery.run(
            options,
            () -> TextIOToBigQuery.writeToBQTransform(options).withTestServices(bigQueryServices))
        .waitUntilFinish();
    assertThat(fakeDatasetService.getAllRows(PROJECT, DATASET, TABLE))
        .isEqualTo(
            ImmutableList.of(
                new TableRow()
                    .set("BOOK_ID", "1")
                    .set("TITLE", "ABC")
                    .set(
                        "DETAILS",
                        new TableRow()
                            .set("YEAR", 2023)
                            .set("SUMMARY", "LOREM IPSUM LOREM IPSUM"))));
  }

  @Test
  @Ignore("Flaky test, investigate & re-enable")
  public void testE2EWithStorageWriteApiAtLeastOnce() throws IOException, InterruptedException {
    options.setUseStorageWriteApi(true);
    options.setUseStorageWriteApiAtLeastOnce(true);

    TextIOToBigQuery.run(
            options,
            () -> TextIOToBigQuery.writeToBQTransform(options).withTestServices(bigQueryServices))
        .waitUntilFinish();
    assertThat(fakeDatasetService.getAllRows(PROJECT, DATASET, TABLE))
        .isEqualTo(
            ImmutableList.of(
                new TableRow()
                    .set("book_id", "1")
                    .set("title", "ABC")
                    .set(
                        "details",
                        new TableRow()
                            .set("year", "2023")
                            .set("summary", "LOREM IPSUM LOREM IPSUM"))));
  }

  @Test
  @Ignore("Flaky test, investigate & re-enable")
  public void testE2EWithStorageWriteApi() throws IOException, InterruptedException {
    options.setUseStorageWriteApi(true);

    TextIOToBigQuery.run(
            options,
            () -> TextIOToBigQuery.writeToBQTransform(options).withTestServices(bigQueryServices))
        .waitUntilFinish();
    assertThat(fakeDatasetService.getAllRows(PROJECT, DATASET, TABLE))
        .isEqualTo(
            ImmutableList.of(
                new TableRow()
                    .set("book_id", "1")
                    .set("title", "ABC")
                    .set(
                        "details",
                        new TableRow()
                            .set("year", "2023")
                            .set("summary", "LOREM IPSUM LOREM IPSUM"))));
  }
}
