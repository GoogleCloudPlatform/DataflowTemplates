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
package com.google.cloud.teleport.v2.templates.mysql;

import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.sink.TypeMapper;
import java.util.Locale;
import javax.annotation.Nullable;

/** TypeMapper implementation for JDBC types. */
public class MySqlTypeMapper implements TypeMapper {

  @Override
  public LogicalType getLogicalType(
      String typeName, @Nullable Object dialect, @Nullable Long size) {
    if (typeName == null) {
      return LogicalType.STRING;
    }

    String upperType = typeName.toUpperCase(Locale.ROOT);
    // Remove parameters
    if (upperType.contains("(")) {
      upperType = upperType.substring(0, upperType.indexOf("("));
    }

    switch (upperType) {
      case "BIT":
      case "BOOLEAN":
      case "BOOL":
        return LogicalType.BOOLEAN;
      case "TINYINT":
        return (size != null && size == 1) ? LogicalType.BOOLEAN : LogicalType.INT64;
      case "SMALLINT":
      case "MEDIUMINT":
      case "INTEGER":
      case "INT":
      case "BIGINT":
        return LogicalType.INT64;

      case "FLOAT":
      case "REAL":
      case "DOUBLE":
        return LogicalType.FLOAT64;

      case "NUMERIC":
      case "DECIMAL":
        return LogicalType.NUMERIC;

      case "CHAR":
      case "VARCHAR":
      case "LONGVARCHAR":
      case "TEXT":
      case "MEDIUMTEXT":
      case "LONGTEXT":
      case "ENUM":
      case "SET":
        return LogicalType.STRING;

      case "BINARY":
      case "VARBINARY":
      case "LONGVARBINARY":
      case "BLOB":
      case "MEDIUMBLOB":
      case "LONGBLOB":
      case "TINYBLOB":
        return LogicalType.BYTES;

      case "DATE":
        return LogicalType.DATE;

      case "TIME":
      case "TIMESTAMP":
      case "DATETIME":
        return LogicalType.TIMESTAMP;

      case "JSON":
        return LogicalType.JSON;

      default:
        return LogicalType.STRING;
    }
  }
}
