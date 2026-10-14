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
package com.google.cloud.teleport.v2.templates.sqlserver;

import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.sink.TypeMapper;
import java.util.Locale;
import javax.annotation.Nullable;

/**
 * {@link TypeMapper} implementation for SQL Server column types.
 *
 * <p>Type names are expected in the form returned by JDBC {@code DatabaseMetaData.getColumns()} for
 * the Microsoft driver, e.g. {@code "int"}, {@code "nvarchar"}, {@code "decimal"} or {@code "int
 * identity"} (identity columns carry an {@code identity} suffix which is ignored here).
 */
public class SqlServerTypeMapper implements TypeMapper {

  @Override
  public LogicalType getLogicalType(
      String typeName, @Nullable Object dialect, @Nullable Long size) {
    if (typeName == null) {
      throw new IllegalArgumentException("Type name cannot be null");
    }

    String upperType = typeName.toUpperCase(Locale.ROOT).trim();
    // Remove parameters e.g. DECIMAL(10,2)
    if (upperType.contains("(")) {
      upperType = upperType.substring(0, upperType.indexOf("("));
    }
    // Remove the "identity" suffix reported by the SQL Server JDBC driver, e.g. "INT IDENTITY".
    int space = upperType.indexOf(' ');
    if (space > 0) {
      upperType = upperType.substring(0, space);
    }

    switch (upperType) {
      case "BIT":
        return LogicalType.BOOLEAN;

      case "TINYINT":
      case "SMALLINT":
      case "INT":
      case "INTEGER":
      case "BIGINT":
        return LogicalType.INT64;

      case "FLOAT":
      case "REAL":
        return LogicalType.FLOAT64;

      case "DECIMAL":
      case "NUMERIC":
      case "MONEY":
      case "SMALLMONEY":
        return LogicalType.NUMERIC;

      case "CHAR":
      case "NCHAR":
      case "VARCHAR":
      case "NVARCHAR":
      case "TEXT":
      case "NTEXT":
      case "XML":
      case "SYSNAME":
        return LogicalType.STRING;

      case "BINARY":
      case "VARBINARY":
      case "IMAGE":
      case "TIMESTAMP":
      case "ROWVERSION":
        return LogicalType.BYTES;

      case "DATE":
        return LogicalType.DATE;

      case "TIME":
      case "DATETIME":
      case "DATETIME2":
      case "SMALLDATETIME":
      case "DATETIMEOFFSET":
        return LogicalType.TIMESTAMP;

      case "UNIQUEIDENTIFIER":
        return LogicalType.UUID;

      default:
        return LogicalType.STRING;
    }
  }
}
