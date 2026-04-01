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
package com.google.cloud.teleport.v2.spanner.migrations.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SpannerReadUtils {
  // TODO: After beam release, use the latest client lib version which supports
  // setting lock
  // hints via the read api. SQL string generation should be removed.
  public static Statement generateReadSQLWithExclusiveLock(
      String tableName, List<String> readColumnList, Key primaryKey, Ddl ddl) {
    String columnNames = String.join(", ", readColumnList);
    // TODO: Handle json type as PKs.
    boolean isPostgres = ddl.dialect() == Dialect.POSTGRESQL;
    String whereClause;
    if (isPostgres) {
      List<String> conditions = new ArrayList<>();
      List<IndexColumn> pks = ddl.table(tableName).primaryKeys();
      for (int i = 0; i < pks.size(); i++) {
        conditions.add(quoteIdentifier(pks.get(i).name(), ddl.dialect()) + "=$" + (i + 1));
      }
      whereClause = String.join(" AND ", conditions);
    } else {
      whereClause =
          String.join(
              " AND ",
              ddl.table(tableName).primaryKeys().stream()
                  .map(col -> col.name() + "=@" + col.name())
                  .collect(Collectors.toList()));
    }

    String sql =
        (isPostgres ? "/*@ LOCK_SCANNED_RANGES=exclusive */ " : "@{LOCK_SCANNED_RANGES=exclusive} ")
            + "SELECT "
            + columnNames
            + " FROM "
            + quoteIdentifier(tableName, ddl.dialect())
            + " WHERE "
            + whereClause;

    Statement.Builder stmtBuilder = Statement.newBuilder(sql);
    int i = 0;
    for (Object value : primaryKey.getParts()) {
      Table table = ddl.table(tableName);
      String colName = table.primaryKeys().get(i).name();
      Column key = table.column(colName);
      Type keyColType = key.type();

      String bindName = isPostgres ? "p" + (i + 1) : colName;

      switch (keyColType.getCode()) {
        case BOOL:
        case PG_BOOL:
          stmtBuilder.bind(bindName).to((Boolean) value);
          break;
        case INT64:
        case PG_INT8:
          stmtBuilder.bind(bindName).to((Long) value);
          break;
        case FLOAT64:
        case PG_FLOAT8:
          stmtBuilder.bind(bindName).to((Double) value);
          break;
        case STRING:
        case PG_VARCHAR:
        case PG_TEXT:
        case JSON:
        case PG_JSONB:
          stmtBuilder.bind(bindName).to((String) value);
          break;
        case NUMERIC:
          stmtBuilder.bind(bindName).to((BigDecimal) value);
          break;
        case PG_NUMERIC:
          stmtBuilder.bind(bindName).to(Value.pgNumeric(value.toString()));
          break;
        case BYTES:
        case PG_BYTEA:
          stmtBuilder.bind(bindName).to((ByteArray) value);
          break;
        case TIMESTAMP:
        case PG_COMMIT_TIMESTAMP:
        case PG_TIMESTAMPTZ:
          stmtBuilder.bind(bindName).to((Timestamp) value);
          break;
        case DATE:
        case PG_DATE:
          stmtBuilder.bind(bindName).to((Date) value);
          break;
        default:
          throw new IllegalArgumentException("Unsupported type: " + keyColType);
      }
      i++;
    }
    return stmtBuilder.build();
  }

  private static String quoteIdentifier(String name, Dialect dialect) {
    return (dialect == Dialect.POSTGRESQL) ? "\"" + name + "\"" : name;
  }
}
