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
package com.google.cloud.teleport.v2.values;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Represent a Cloud Spanner Schema. The key is the column name and the value is data type */
public class SpannerSchema {
  /** This enum represents the Cloud Spanner data types that supported by this Dataflow pipeline. */
  public enum SpannerDataTypes {
    INT64,
    FLOAT64,
    BOOL,
    DATE,
    TIMESTAMP,
    STRING
  };

  private static final Logger LOG = LoggerFactory.getLogger(SpannerSchema.class);
  private static final String SCHEMA_COLUMN_DELIMITER = ",";
  private static final String SCHEMA_DELIMITER = ":";
  private static final Pattern STRING_PATTERN = Pattern.compile("STRING(?:\\((?:MAX|[0-9]+)\\))?");
  Map<String, SpannerDataTypes> spannerSchema = new LinkedHashMap<>();

  public SpannerSchema(String schemaString) {
    for (String pair : schemaString.split(SCHEMA_COLUMN_DELIMITER)) {
      String[] columnPair = pair.split(SCHEMA_DELIMITER);
      spannerSchema.put(columnPair[0].trim(), parseSpannerDataType(columnPair[1].trim()));
    }
  }

  public SpannerSchema() {}

  public static SpannerDataTypes parseSpannerDataType(String columnType) {
    if (STRING_PATTERN.matcher(columnType).matches()) {
      return SpannerDataTypes.STRING;
    } else if (columnType.equalsIgnoreCase("FLOAT64")) {
      return SpannerDataTypes.FLOAT64;
    } else if (columnType.equalsIgnoreCase("INT64")) {
      return SpannerDataTypes.INT64;
    } else if (columnType.equalsIgnoreCase("BOOL")) {
      return SpannerDataTypes.BOOL;
    } else if (columnType.equalsIgnoreCase("DATE")) {
      return SpannerDataTypes.DATE;
    } else if (columnType.equalsIgnoreCase("TIMESTAMP")) {
      return SpannerDataTypes.TIMESTAMP;
    } else {
      throw new IllegalArgumentException(
          "Unrecognized or unsupported column data type: " + columnType);
    }
  }

  public void addEntry(String columnName, SpannerDataTypes columnType) {
    spannerSchema.put(columnName, columnType);
  }

  public void addEntry(String columnName, String columnType) {
    spannerSchema.put(columnName, parseSpannerDataType(columnType));
  }

  public SpannerDataTypes getColumnType(String key) {
    return this.spannerSchema.get(key);
  }

  public List<String> getColumnList() {
    List<String> result = new ArrayList<>();
    result.addAll(spannerSchema.keySet());

    return result;
  }

  public boolean compatibleWithSchema(SpannerSchema targetSpannerSchema) {
    boolean result = true;
    for (String columnName : spannerSchema.keySet()) {
      if (!targetSpannerSchema.spannerSchema.containsKey(columnName)) {
        LOG.info("Schema does not contain column: " + columnName);
        result = false;
      }

      if (getColumnType(columnName) != targetSpannerSchema.getColumnType(columnName)) {
        LOG.info(
            "In schema, column: {} does not has the same data type. Source type: {}, the other type: {}",
            columnName,
            getColumnType(columnName),
            targetSpannerSchema.getColumnType(columnName));
        result = false;
      }
    }
    return result;
  }
}
