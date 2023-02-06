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
package com.google.cloud.teleport.it.matchers;

import static com.google.common.truth.Truth.assertAbout;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.it.artifacts.Artifact;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.jetbrains.annotations.Nullable;

/** Assert utilities for Template DSL-like tests. */
public final class TemplateAsserts {

  private static TypeReference<Map<String, Object>> recordTypeReference = new TypeReference<>() {};

  private static ObjectMapper objectMapper = new ObjectMapper();

  /**
   * Creates a {@link LaunchInfoSubject} to assert information returned from pipeline launches.
   *
   * @param launchInfo Launch information returned from the launcher.
   * @return Truth Subject to chain assertions.
   */
  public static LaunchInfoSubject assertThatPipeline(LaunchInfo launchInfo) {
    return assertAbout(LaunchInfoSubject.launchInfo()).that(launchInfo);
  }

  /**
   * Creates a {@link ResultSubject} to add assertions based on a pipeline result.
   *
   * @param result Pipeline result returned from the launcher.
   * @return Truth Subject to chain assertions.
   */
  public static ResultSubject assertThatResult(Result result) {
    return assertAbout(ResultSubject.result()).that(result);
  }

  /**
   * Creates a {@link RecordsSubject} to assert information within a list of records.
   *
   * @param records Records in a map list format to use in the comparison.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatRecords(@Nullable List<Map<String, Object>> records) {
    return assertAbout(RecordsSubject.records()).that(records);
  }

  /**
   * Creates a {@link RecordsSubject} to assert information within a list of records.
   *
   * @param tableResult Records in BigQuery {@link TableResult} format to use in the comparison.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatRecords(@Nullable TableResult tableResult) {
    return assertThatRecords(tableResultToRecords(tableResult));
  }

  /**
   * Creates a {@link RecordsSubject} to assert information within a list of records.
   *
   * @param structs Records in Spanner {@link Struct} format to use in the comparison.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatStructs(List<Struct> structs) {
    return assertThatRecords(structsToRecords(structs));
  }

  /**
   * Creates a {@link RecordsSubject} to assert information within a list of records.
   *
   * @param records Records in Avro/Parquet {@link GenericRecord} format to use in the comparison.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatGenericRecords(List<GenericRecord> records) {
    return assertThatRecords(genericRecordToRecords(records));
  }

  /**
   * Creates a {@link ArtifactsSubject} to assert information within a list of artifacts obtained
   * from Cloud Storage.
   *
   * @param artifacts Artifacts in list format to use in the comparisons.
   * @return Truth Subject to chain assertions.
   */
  public static ArtifactsSubject assertThatArtifacts(@Nullable List<Artifact> artifacts) {
    return assertAbout(ArtifactsSubject.records()).that(artifacts);
  }

  /**
   * Creates a {@link ArtifactsSubject} to assert information for an artifact obtained from Cloud
   * Storage.
   *
   * @param artifact Artifact to use in the comparisons.
   * @return Truth Subject to chain assertions.
   */
  public static ArtifactsSubject assertThatArtifact(@Nullable Artifact artifact) {
    return assertAbout(ArtifactsSubject.records()).that(List.of(artifact));
  }

  /**
   * Convert BigQuery {@link TableResult} to a list of maps.
   *
   * @param tableResult Table Result to parse
   * @return List of maps to use in {@link RecordsSubject}
   */
  private static List<Map<String, Object>> tableResultToRecords(TableResult tableResult) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (FieldValueList row : tableResult.iterateAll()) {
        String jsonRow = row.get(0).getStringValue();
        Map<String, Object> converted = objectMapper.readValue(jsonRow, recordTypeReference);
        records.add(converted);
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting TableResult to Records", e);
    }
  }

  /**
   * Convert Spanner {@link Struct} list to a list of maps.
   *
   * @param structs Structs to parse
   * @return List of maps to use in {@link RecordsSubject}
   */
  private static List<Map<String, Object>> structsToRecords(List<Struct> structs) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (Struct struct : structs) {
        Map<String, Object> record = new HashMap<>();

        for (StructField field : struct.getType().getStructFields()) {
          Value fieldValue = struct.getValue(field.getName());
          // May need to explore using typed methods instead of .toString()
          record.put(field.getName(), fieldValue.toString());
        }

        records.add(record);
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting TableResult to Records", e);
    }
  }

  /**
   * Convert Avro {@link GenericRecord} to a list of maps.
   *
   * @param avroRecords Avro Records to parse
   * @return List of maps to use in {@link RecordsSubject}
   */
  private static List<Map<String, Object>> genericRecordToRecords(List<GenericRecord> avroRecords) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (GenericRecord row : avroRecords) {
        Map<String, Object> converted = objectMapper.readValue(row.toString(), recordTypeReference);
        records.add(converted);
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting Avro Record to Map", e);
    }
  }
}
