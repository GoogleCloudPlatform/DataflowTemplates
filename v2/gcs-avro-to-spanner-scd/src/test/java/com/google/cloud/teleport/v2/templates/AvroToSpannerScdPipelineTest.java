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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.templates.AvroToSpannerScdPipeline.AvroToSpannerScdOptions;
import com.google.cloud.teleport.v2.templates.AvroToSpannerScdPipeline.AvroToSpannerScdOptions.ScdType;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class AvroToSpannerScdPipelineTest {

  AvroToSpannerScdOptions pipelineOptions;

  @Before
  public void setUp() {
    // Required fields only.
    pipelineOptions = PipelineOptionsFactory.as(AvroToSpannerScdOptions.class);
    pipelineOptions.setInputFilePattern("gs://bucket/file-path/*");
    pipelineOptions.setInstanceId("spanner-instance-id");
    pipelineOptions.setDatabaseId("spanner-database-id");
    pipelineOptions.setTableName("spanner-table");
    pipelineOptions.setPrimaryKeyColumnNames(List.of("id", "key"));
  }

  @Test
  public void testValidateOptions_passesWithDefaultFields() {
    AvroToSpannerScdPipeline.validateOptions(pipelineOptions);
    // Do not throw.
  }

  @Test
  public void testValidateOptions_throwsEmptySpannerProjectId() {
    pipelineOptions.setSpannerProjectId("");

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> AvroToSpannerScdPipeline.validateOptions(pipelineOptions));

    assertThat(thrown).hasMessageThat().contains("Spanner project id must not be empty.");
  }

  @Test
  public void testValidateOptions_throwsEmptyInstanceId() {
    pipelineOptions.setInstanceId("");

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> AvroToSpannerScdPipeline.validateOptions(pipelineOptions));

    assertThat(thrown).hasMessageThat().contains("Spanner instance id must not be empty.");
  }

  @Test
  public void testValidateOptions_throwsEmptySpannerDatabaseId() {
    pipelineOptions.setDatabaseId("");

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> AvroToSpannerScdPipeline.validateOptions(pipelineOptions));

    assertThat(thrown).hasMessageThat().contains("Spanner database id must not be empty.");
  }

  @Test
  public void testValidateOptions_throwsNegativeBatchSize() {
    pipelineOptions.setSpannerBatchSize(-5);

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> AvroToSpannerScdPipeline.validateOptions(pipelineOptions));

    assertThat(thrown)
        .hasMessageThat()
        .contains("Batch size must be greater than 0. Provided: -5.");
  }

  @Test
  public void testValidateOptions_throwsEmptyTableName() {
    pipelineOptions.setTableName("");

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> AvroToSpannerScdPipeline.validateOptions(pipelineOptions));

    assertThat(thrown).hasMessageThat().contains("Spanner table name must not be empty.");
  }

  @Test
  public void testValidateOptions_throwsEmptyPrimaryKeyColumnNames() {
    pipelineOptions.setPrimaryKeyColumnNames(List.of());

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> AvroToSpannerScdPipeline.validateOptions(pipelineOptions));

    assertThat(thrown)
        .hasMessageThat()
        .contains("Spanner primary key column names must not be empty.");
  }

  @Test
  public void testValidateOptions_throwsEmptyOrderByColumnName() {
    pipelineOptions.setOrderByColumnName("");

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> AvroToSpannerScdPipeline.validateOptions(pipelineOptions));

    assertThat(thrown)
        .hasMessageThat()
        .contains("When provided, order by column name must not be empty.");
  }

  @Test
  public void testValidateOptions_scdType1_throwsIfUsingStartDate() {
    pipelineOptions.setScdType(ScdType.TYPE_1);
    pipelineOptions.setStartDateColumnName("start_date");

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> AvroToSpannerScdPipeline.validateOptions(pipelineOptions));

    assertThat(thrown)
        .hasMessageThat()
        .contains("When using SCD Type 1, start date column name is not used.");
  }

  @Test
  public void testValidateOptions_scdType1_throwsIfUsingEndDate() {
    pipelineOptions.setScdType(ScdType.TYPE_1);
    pipelineOptions.setEndDateColumnName("end_date");

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> AvroToSpannerScdPipeline.validateOptions(pipelineOptions));

    assertThat(thrown)
        .hasMessageThat()
        .contains("When using SCD Type 1, end date column name is not used.");
  }

  @Test
  public void testValidateOptions_scdType2_throwsEmptyStartDate() {
    pipelineOptions.setScdType(ScdType.TYPE_2);
    pipelineOptions.setStartDateColumnName("");
    pipelineOptions.setEndDateColumnName("end_date");

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> AvroToSpannerScdPipeline.validateOptions(pipelineOptions));

    assertThat(thrown)
        .hasMessageThat()
        .contains("When provided, start date column name must not be empty.");
  }

  @Test
  public void testValidateOptions_scdType2_throwsNoEndDate() {
    pipelineOptions.setScdType(ScdType.TYPE_2);

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> AvroToSpannerScdPipeline.validateOptions(pipelineOptions));

    assertThat(thrown)
        .hasMessageThat()
        .contains("When using SCD Type 2, end date column name must be provided.");
  }

  @Test
  public void testValidateOptions_scdType2_throwsEmptyEndDate() {
    pipelineOptions.setScdType(ScdType.TYPE_2);
    pipelineOptions.setEndDateColumnName("");

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> AvroToSpannerScdPipeline.validateOptions(pipelineOptions));

    assertThat(thrown)
        .hasMessageThat()
        .contains("When using SCD Type 2, end date column name must not be empty.");
  }
}
