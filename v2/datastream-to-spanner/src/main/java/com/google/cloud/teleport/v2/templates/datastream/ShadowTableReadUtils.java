/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.datastream;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Statement;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

public class ShadowTableReadUtils {
  // TODO: After beam release, use the latest client lib version which supports setting lock
  // hints via the read api. SQL string generation should be removed.
  public static Statement generateShadowTableReadSQL(
      String shadowTable, List<String> readColumnList, Key primaryKey, Ddl shadowTableDdl) {
    String columnNames = String.join(", ", readColumnList);
    // TODO: Handle json type as PKs.
    String whereClause =
        String.join(
            " AND ",
            shadowTableDdl.table(shadowTable).primaryKeys().stream()
                .map(col -> col.name() + "=@" + col.name())
                .collect(Collectors.toList()));
    String sql =
        "@{LOCK_SCANNED_RANGES=exclusive} SELECT "
            + columnNames
            + " FROM "
            + shadowTable
            + " WHERE "
            + whereClause;

    Statement.Builder stmtBuilder = Statement.newBuilder(sql);
    int i = 0;
    for (Object value : primaryKey.getParts()) {
      Table table = shadowTableDdl.table(shadowTable);
      String colName = table.primaryKeys().get(i).name();
      Column key = table.column(colName);
      Type keyColType = key.type();

      switch (keyColType.getCode()) {
        case BOOL:
        case PG_BOOL:
          stmtBuilder.bind(colName).to((Boolean) value);
          break;
        case INT64:
        case PG_INT8:
          stmtBuilder.bind(colName).to((Long) value);
          break;
        case FLOAT64:
        case PG_FLOAT8:
          stmtBuilder.bind(colName).to((Double) value);
          break;
        case STRING:
        case PG_VARCHAR:
        case PG_TEXT:
        case JSON:
        case PG_JSONB:
          stmtBuilder.bind(colName).to((String) value);
          break;
        case NUMERIC:
        case PG_NUMERIC:
          stmtBuilder.bind(colName).to((BigDecimal) value);
          break;
        case BYTES:
        case PG_BYTEA:
          stmtBuilder.bind(colName).to((ByteArray) value);
          break;
        case TIMESTAMP:
        case PG_COMMIT_TIMESTAMP:
        case PG_TIMESTAMPTZ:
          stmtBuilder.bind(colName).to((Timestamp) value);
          break;
        case DATE:
        case PG_DATE:
          stmtBuilder.bind(colName).to((Date) value);
          break;
        default:
          throw new IllegalArgumentException("Unsupported type: " + keyColType);
      }
      i++;
    }
    return stmtBuilder.build();
  }
}
