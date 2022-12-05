/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Test cases for the {@link BigQueryIOUtils} class. */
public class BigQueryIOUtilsTest {

  private BigQueryOptions options;

  private static final String PROJECT = "test-project";
  private static final String DATASET = "test-dataset";
  private static final String TABLE = "test-table";
  public static final TableRow TABLE_ROW = new TableRow().set("BOOK_ID", "1").set("TITLE", "ABC");
  /** A row with wrong field that should cause "TableRow contained unexpected field" error. */
  public static final TableRow TABLE_ROW_WITH_WRONG_FIELD =
      new TableRow().set("BOOK_ID", "1").set("WRONG_FIELD", "ABC");

  @Rule public final TemporaryFolder tmp = new TemporaryFolder();

  private FakeDatasetService fakeDatasetService;
  private BigQueryServices bigQueryServices;

  @Before
  public void setUp() throws IOException, InterruptedException {
    // setup BQ
    FakeDatasetService.setUp();
    fakeDatasetService = new FakeDatasetService();
    fakeDatasetService.createDataset(PROJECT, DATASET, "", "", null);
    TableSchema bqSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("BOOK_ID").setType("STRING"),
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
    options = PipelineOptionsFactory.as(BigQueryOptions.class);
    options.setJobName("test-job"); // otherwise, the job name can randomly change during execution
  }

  @Test
  public void testValidateBqOptionsStorageApiOff() {
    BigQueryIOUtils.validateBQStorageApiOptionsBatch(options);
  }

  @Test
  public void testValidateBqOptionsBatchAtMostOnce() {
    options.setUseStorageWriteApi(true);

    BigQueryIOUtils.validateBQStorageApiOptionsBatch(options);
  }

  @Test
  public void testValidateBqOptionsBatchAtLeastOnce() {
    options.setUseStorageWriteApi(true);
    options.setUseStorageWriteApiAtLeastOnce(true);

    BigQueryIOUtils.validateBQStorageApiOptionsBatch(options);
  }

  @Test
  public void testValidateBqOptionsBatchAtLeastOnceInvalid() {
    options.setUseStorageWriteApiAtLeastOnce(true);

    assertThrows(
        IllegalArgumentException.class,
        () -> BigQueryIOUtils.validateBQStorageApiOptionsBatch(options));
  }

  @Test
  public void testValidateBqOptionsStreamingAtMostOnce() {
    options.setUseStorageWriteApi(true);
    options.setNumStorageWriteApiStreams(3);
    options.setStorageWriteApiTriggeringFrequencySec(33);

    BigQueryIOUtils.validateBQStorageApiOptionsStreaming(options);
  }

  @Test
  public void testValidateBqOptionsStreamingAtLeastOnce() {
    options.setUseStorageWriteApi(true);
    options.setUseStorageWriteApiAtLeastOnce(true);

    BigQueryIOUtils.validateBQStorageApiOptionsStreaming(options);
  }

  @Test
  public void testValidateBqOptionsStreamingAtLeastOnceInvalid() {
    BigQueryOptions options = PipelineOptionsFactory.as(BigQueryOptions.class);
    options.setUseStorageWriteApiAtLeastOnce(true);

    assertThrows(
        IllegalArgumentException.class,
        () -> BigQueryIOUtils.validateBQStorageApiOptionsStreaming(options));
  }

  @Test
  public void testValidateBqOptionsStreamingAtMostOnceInvalidNoStreams() {
    BigQueryOptions options = PipelineOptionsFactory.as(BigQueryOptions.class);
    options.setUseStorageWriteApiAtLeastOnce(true);
    options.setStorageWriteApiTriggeringFrequencySec(33);

    assertThrows(
        IllegalArgumentException.class,
        () -> BigQueryIOUtils.validateBQStorageApiOptionsStreaming(options));
  }

  @Test
  public void testValidateBqOptionsStreamingAtMostOnceInvalidNoFrequency() {
    options = PipelineOptionsFactory.as(BigQueryOptions.class);
    options.setUseStorageWriteApiAtLeastOnce(true);
    options.setNumStorageWriteApiStreams(3);

    assertThrows(
        IllegalArgumentException.class,
        () -> BigQueryIOUtils.validateBQStorageApiOptionsStreaming(options));
  }

  @Test
  public void testWriteResultToBigQueryInsertErrorsWithStreamingInserts() {
    WriteResult writeResult = generateWriteResultsWithError(Pipeline.create(options), TABLE_ROW);

    assertThat(BigQueryIOUtils.writeResultToBigQueryInsertErrors(writeResult, options))
        .isSameInstanceAs(writeResult.getFailedInsertsWithErr());
  }

  @Test
  public void testWriteResultToBigQueryInsertErrorsWithStorageApi() {
    options.setUseStorageWriteApi(true);
    options.setNumStorageWriteApiStreams(1);
    options.setStorageWriteApiTriggeringFrequencySec(1);
    Pipeline pipeline = Pipeline.create(options);
    WriteResult writeResult = generateWriteResultsWithError(pipeline, TABLE_ROW_WITH_WRONG_FIELD);

    PAssert.thatSingleton(BigQueryIOUtils.writeResultToBigQueryInsertErrors(writeResult, options))
        .satisfies(
            e -> {
              assertThat(e.getRow()).isEqualTo(TABLE_ROW_WITH_WRONG_FIELD);
              assertThat(e.getError().getErrors()).hasSize(1);
              assertThat(e.getError().getErrors().get(0).getMessage())
                  .contains("TableRow contained unexpected field with name");
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testWriteResultToBigQueryInsertErrorsWithStorageApiNoErrors() {
    options.setUseStorageWriteApi(true);
    options.setNumStorageWriteApiStreams(1);
    options.setStorageWriteApiTriggeringFrequencySec(1);
    Pipeline pipeline = Pipeline.create(options);
    WriteResult writeResult = generateWriteResultsWithError(pipeline, TABLE_ROW);

    PAssert.that(BigQueryIOUtils.writeResultToBigQueryInsertErrors(writeResult, options)).empty();

    pipeline.run();
  }

  private WriteResult generateWriteResultsWithError(Pipeline pipeline, TableRow tableRow) {
    WriteResult writeResult =
        pipeline
            .apply(
                TestStream.create(TableRowJsonCoder.of())
                    .addElements(tableRow)
                    .advanceWatermarkToInfinity())
            .apply(
                BigQueryIO.writeTableRows()
                    .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                    .withExtendedErrorInfo()
                    .to(
                        new TableReference()
                            .setProjectId(PROJECT)
                            .setDatasetId(DATASET)
                            .setTableId(TABLE))
                    .withTestServices(bigQueryServices));
    return writeResult;
  }
}
