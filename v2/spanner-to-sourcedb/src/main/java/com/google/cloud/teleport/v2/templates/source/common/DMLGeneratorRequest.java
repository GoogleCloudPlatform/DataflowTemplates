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
package com.google.cloud.teleport.v2.templates.source.common;

import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import org.json.JSONObject;

public class DMLGeneratorRequest {
  private String modType;
  private String spannerTableName;
  private Schema schema;
  private JSONObject newValuesJson;
  private JSONObject keyValuesJson;
  private String sourceDbTimezoneOffset;

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

  public DMLGeneratorRequest(
      String modType,
      String spannerTableName,
      Schema schema,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset) {
    this.modType = modType;
    this.spannerTableName = spannerTableName;
    this.schema = schema;
    this.newValuesJson = newValuesJson;
    this.keyValuesJson = keyValuesJson;
    this.sourceDbTimezoneOffset = sourceDbTimezoneOffset;
  }
}
