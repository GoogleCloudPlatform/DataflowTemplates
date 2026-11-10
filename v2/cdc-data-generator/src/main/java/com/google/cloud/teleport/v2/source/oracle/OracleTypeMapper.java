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
package com.google.cloud.teleport.v2.source.oracle;

import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.sink.TypeMapper;
import java.util.Locale;
import javax.annotation.Nullable;

/** TypeMapper implementation for Oracle database types. */
public class OracleTypeMapper implements TypeMapper {

  @Override
  public LogicalType getLogicalType(
      String typeName, @Nullable Object dialect, @Nullable Long size) {
    if (typeName == null) {
      throw new IllegalArgumentException("Type name cannot be null");
    }

    // Normalize type name by stripping parenthesized parameters (e.g. "TIMESTAMP(6) WITH TIME ZONE"
    // -> "TIMESTAMP WITH TIME ZONE", "INTERVAL YEAR(2) TO MONTH" -> "INTERVAL YEAR TO MONTH").
    String upperType =
        typeName
            .toUpperCase(Locale.ROOT)
            .replaceAll("\\s*\\([^)]*\\)", "")
            .replaceAll("\\s+", " ")
            .trim();

    switch (upperType) {
      case "BOOLEAN":
      case "BOOL":
        return LogicalType.BOOLEAN;

      case "INTEGER":
      case "INT":
      case "SMALLINT":
        return LogicalType.INT64;

      case "FLOAT":
      case "DOUBLE PRECISION":
      case "REAL":
      case "BINARY_FLOAT":
      case "BINARY_DOUBLE":
        return LogicalType.FLOAT64;

      case "NUMBER":
      case "NUMERIC":
      case "DECIMAL":
      case "DEC":
        return LogicalType.NUMERIC;

      case "VARCHAR2":
      case "VARCHAR":
      case "CHAR":
      case "CHARACTER":
      case "NVARCHAR2":
      case "NCHAR":
      case "NCHAR VARYING":
      case "NATIONAL CHARACTER":
      case "NATIONAL CHAR":
      case "NATIONAL CHARACTER VARYING":
      case "NATIONAL CHAR VARYING":
      case "CLOB":
      case "NCLOB":
      case "BFILE":
      case "LONG":
      case "ROWID":
      case "UROWID":
      case "INTERVAL YEAR TO MONTH":
      case "INTERVAL DAY TO SECOND":
      case "XMLTYPE":
      case "SDO_GEOMETRY":
      case "SDO_TOPO_GEOMETRY":
      case "URITYPE":
      case "DBURITYPE":
      case "XDBURITYPE":
      case "HTTPURITYPE":
      case "EXPRESSION":
      case "MLSLABEL":
      case "REF":
        return LogicalType.STRING;

      case "RAW":
      case "LONG RAW":
      case "BLOB":
      case "SDO_GEORASTER":
      case "ANYDATA":
      case "ANYTYPE":
      case "ANYDATASET":
      case "ORDAUDIO":
      case "ORDIMAGE":
      case "ORDVIDEO":
      case "ORDDOC":
      case "SI_STILLIMAGE":
      case "SI_COLOR":
      case "SI_AVERAGECOLOR":
      case "SI_COLORHISTOGRAM":
      case "SI_POSITIONALCOLOR":
      case "SI_TEXTURE":
      case "SI_FEATURELIST":
        return LogicalType.BYTES;

      case "DATE":
      case "TIMESTAMP":
      case "TIMESTAMP WITH TIME ZONE":
      case "TIMESTAMP WITH LOCAL TIME ZONE":
        return LogicalType.TIMESTAMP;

      case "JSON":
        return LogicalType.JSON;

      default:
        return LogicalType.STRING;
    }
  }
}
