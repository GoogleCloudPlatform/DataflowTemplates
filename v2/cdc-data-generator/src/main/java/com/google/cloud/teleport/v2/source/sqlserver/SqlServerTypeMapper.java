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
package com.google.cloud.teleport.v2.source.sqlserver;

import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.sink.TypeMapper;
import java.util.Locale;
import javax.annotation.Nullable;

/**
 * TypeMapper implementation for SQL Server database types.
 *
 * <p>Accepts the raw JDBC {@code TYPE_NAME} reported by the SQL Server driver (for example {@code
 * int identity}, {@code nvarchar}, {@code decimal() identity}) as well as the documented synonyms
 * and parameterised spellings (for example {@code NATIONAL CHAR VARYING}, {@code FLOAT(24)}, {@code
 * VARCHAR(MAX)}).
 */
public class SqlServerTypeMapper implements TypeMapper {

  private static final String IDENTITY_SUFFIX = " IDENTITY";

  /**
   * Normalises a SQL Server type name: upper-cases it, removes the {@code identity} suffix that the
   * driver appends for IDENTITY columns, strips parenthesised parameters and collapses whitespace.
   * For example {@code "decimal() identity"} becomes {@code "DECIMAL"} and {@code "varchar(max)"}
   * becomes {@code "VARCHAR"}.
   */
  public static String normalizeTypeName(String typeName) {
    String upper = typeName.toUpperCase(Locale.ROOT).replaceAll("\\s+", " ").trim();
    if (upper.endsWith(IDENTITY_SUFFIX)) {
      upper = upper.substring(0, upper.length() - IDENTITY_SUFFIX.length());
    }
    return upper.replaceAll("\\s*\\([^)]*\\)", "").replaceAll("\\s+", " ").trim();
  }

  /**
   * Returns true if the raw JDBC type name marks an IDENTITY column (e.g. {@code int identity}).
   */
  public static boolean isIdentityTypeName(@Nullable String typeName) {
    return typeName != null
        && typeName
            .toUpperCase(Locale.ROOT)
            .replaceAll("\\s+", " ")
            .trim()
            .endsWith(IDENTITY_SUFFIX);
  }

  @Override
  public LogicalType getLogicalType(
      String typeName, @Nullable Object dialect, @Nullable Long size) {
    if (typeName == null) {
      throw new IllegalArgumentException("Type name cannot be null");
    }

    switch (normalizeTypeName(typeName)) {
      case "BIT":
        return LogicalType.BOOLEAN;

      case "TINYINT":
      case "SMALLINT":
      case "INT":
      case "INTEGER":
      case "BIGINT":
        return LogicalType.INT64;

      case "DECIMAL":
      case "DEC":
      case "NUMERIC":
      case "MONEY":
      case "SMALLMONEY":
        return LogicalType.NUMERIC;

      case "FLOAT":
      case "DOUBLE PRECISION":
      case "REAL":
        return LogicalType.FLOAT64;

      case "CHAR":
      case "CHARACTER":
      case "VARCHAR":
      case "CHAR VARYING":
      case "CHARACTER VARYING":
      case "TEXT":
      case "NCHAR":
      case "NATIONAL CHARACTER":
      case "NATIONAL CHAR":
      case "NVARCHAR":
      case "NATIONAL CHARACTER VARYING":
      case "NATIONAL CHAR VARYING":
      case "NTEXT":
      case "NATIONAL TEXT":
      case "SYSNAME":
      case "XML":
      case "GEOMETRY":
      case "GEOGRAPHY":
      case "HIERARCHYID":
      case "SQL_VARIANT":
        return LogicalType.STRING;

      case "BINARY":
      case "VARBINARY":
      case "BINARY VARYING":
      case "IMAGE":
      case "ROWVERSION":
      case "TIMESTAMP": // SQL Server TIMESTAMP is the synonym of ROWVERSION, not a date/time.
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

      case "JSON":
        return LogicalType.JSON;

      default:
        // Unknown types (e.g. VECTOR, alias or CLR user-defined types) default to STRING. The
        // schema fetcher marks the ones that cannot be generated as skipped.
        return LogicalType.STRING;
    }
  }
}
