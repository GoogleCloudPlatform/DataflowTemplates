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
import org.json.JSONObject;

/**
 * A request object representing the data necessary to generate DML statements for interacting with
 * a source database.
 */
public class DMLGeneratorRequest {
  private final String modType;
  private final String spannerTableName;
  private final Schema schema;
  private final JSONObject newValuesJson;
  private final JSONObject keyValuesJson;
  private final String sourceDbTimezoneOffset;

  public DMLGeneratorRequest(Builder builder) {
    this.modType = builder.modType;
    this.spannerTableName = builder.spannerTableName;
    this.schema = builder.schema;
    this.newValuesJson = builder.newValuesJson;
    this.keyValuesJson = builder.keyValuesJson;
    this.sourceDbTimezoneOffset = builder.sourceDbTimezoneOffset;
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

  public static class Builder {
    private final String modType;
    private final String spannerTableName;
    private final JSONObject newValuesJson;
    private final JSONObject keyValuesJson;
    private final String sourceDbTimezoneOffset;
    private Schema schema;

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

    public DMLGeneratorRequest build() {
      return new DMLGeneratorRequest(this);
    }
  }
}
