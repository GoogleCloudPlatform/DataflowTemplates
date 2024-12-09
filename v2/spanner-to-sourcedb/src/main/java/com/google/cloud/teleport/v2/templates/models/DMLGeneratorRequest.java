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
package com.google.cloud.teleport.v2.templates.models;

import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import java.util.Map;
import org.json.JSONObject;

/**
 * Represents a request object containing the data required to generate DML (Data Manipulation
 * Language) statements for interacting with a source database.
 *
 * <p>This class is immutable and is built using the {@link Builder} class. It includes:
 *
 * <ul>
 *   <li>Modification type (e.g., INSERT, UPDATE, DELETE).
 *   <li>The corresponding Spanner table name.
 *   <li>The schema definition of the table.
 *   <li>JSON objects for new values and key values.
 *   <li>The timezone offset of the source database.
 * </ul>
 */
public class DMLGeneratorRequest {
  // The type of DML operation (e.g., "INSERT", "UPDATE", "DELETE").
  private final String modType;

  // The name of the Spanner table associated with the DML operation.
  private final String spannerTableName;

  // The schema of the source and spanner table, providing details about the table's structure.
  private final Schema schema;

  // JSON object containing the new values for the operation (e.g., updated or inserted values).
  private final JSONObject newValuesJson;

  // JSON object containing the key values for identifying records .
  private final JSONObject keyValuesJson;

  // The timezone offset of the source database, used for handling timezone-specific data.
  private final String sourceDbTimezoneOffset;

  private Map<String, Object> customTransformationResponse;

  public DMLGeneratorRequest(Builder builder) {
    this.modType = builder.modType;
    this.spannerTableName = builder.spannerTableName;
    this.schema = builder.schema;
    this.newValuesJson = builder.newValuesJson;
    this.keyValuesJson = builder.keyValuesJson;
    this.sourceDbTimezoneOffset = builder.sourceDbTimezoneOffset;
    this.customTransformationResponse = builder.customTransformationResponse;
  }

  public String getModType() {
    return modType;
  }

  public String getSpannerTableName() {
    return spannerTableName;
  }

  public Schema getSchema() {
    return schema;
  }

  public JSONObject getNewValuesJson() {
    return newValuesJson;
  }

  public JSONObject getKeyValuesJson() {
    return keyValuesJson;
  }

  public String getSourceDbTimezoneOffset() {
    return sourceDbTimezoneOffset;
  }

  public Map<String, Object> getCustomTransformationResponse() {
    return customTransformationResponse;
  }

  public static class Builder {
    private final String modType;
    private final String spannerTableName;
    private final JSONObject newValuesJson;
    private final JSONObject keyValuesJson;
    private final String sourceDbTimezoneOffset;
    private Schema schema;
    private Map<String, Object> customTransformationResponse;

    public Builder(
        String modType,
        String spannerTableName,
        JSONObject newValuesJson,
        JSONObject keyValuesJson,
        String sourceDbTimezoneOffset) {
      this.modType = modType;
      this.spannerTableName = spannerTableName;
      this.newValuesJson = newValuesJson;
      this.keyValuesJson = keyValuesJson;
      this.sourceDbTimezoneOffset = sourceDbTimezoneOffset;
    }

    public Builder setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public Builder setCustomTransformationResponse(
        Map<String, Object> customTransformationResponse) {
      this.customTransformationResponse = customTransformationResponse;
      return this;
    }

    public DMLGeneratorRequest build() {
      return new DMLGeneratorRequest(this);
    }
  }
}
