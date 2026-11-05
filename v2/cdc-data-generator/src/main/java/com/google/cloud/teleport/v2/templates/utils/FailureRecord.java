/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.time.Instant;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.json.JSONObject;

/**
 * Helper for serialising data-generator failures into newline-delimited JSON for the dead-letter
 * queue.
 *
 * <p>Each record is a flat JSON object with a stable schema so downstream tools (e.g. BigQuery
 * external tables, ad-hoc {@code jq} pipelines) can rely on the field names. Unprintable values
 * (byte arrays, {@link ByteBuffer}s) are hex-encoded; everything else falls back to {@link
 * Object#toString} to keep the failure record self-contained even when the underlying value isn't
 * natively JSON-representable.
 */
public final class FailureRecord {

  /** Operation tag used when the failure happened during row generation, before any sink call. */
  public static final String OPERATION_GENERATION = "GENERATION";

  private FailureRecord() {}

  /**
   * Builds a JSON failure record for a row that could not be generated or written.
   *
   * @param tableName logical table the row belongs to
   * @param operation the in-flight operation: INSERT / UPDATE / DELETE / GENERATION
   * @param row the row that failed; may be {@code null} when the failure happened before the row
   *     could be assembled
   * @param error the throwable that aborted the operation; must not be null
   */
  public static String toJson(String tableName, String operation, Row row, Throwable error) {
    JSONObject obj = new JSONObject();
    obj.put("timestamp", Instant.now().toString());
    obj.put("table", tableName == null ? JSONObject.NULL : tableName);
    obj.put("operation", operation == null ? JSONObject.NULL : operation);
    obj.put("row", rowToJson(row));
    obj.put("error", error == null ? JSONObject.NULL : describeError(error));
    return obj.toString();
  }

  private static Object rowToJson(Row row) {
    if (row == null) {
      return JSONObject.NULL;
    }
    JSONObject obj = new JSONObject();
    Schema schema = row.getSchema();
    for (Schema.Field field : schema.getFields()) {
      Object raw;
      try {
        raw = row.getValue(field.getName());
      } catch (Exception e) {
        raw = "<unreadable: " + e.getClass().getSimpleName() + ">";
      }
      Object val = DataGeneratorUtils.canonicalizeValue(raw);
      obj.put(field.getName(), val);
    }
    return obj;
  }

  private static JSONObject describeError(Throwable error) {
    JSONObject obj = new JSONObject();
    obj.put("class", error.getClass().getName());
    obj.put("message", error.getMessage() == null ? "" : error.getMessage());
    StringWriter sw = new StringWriter();
    error.printStackTrace(new PrintWriter(sw));
    obj.put("stackTrace", sw.toString());
    return obj;
  }
}
