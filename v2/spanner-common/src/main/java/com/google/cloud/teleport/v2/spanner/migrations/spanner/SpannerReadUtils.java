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
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SpannerReadUtils {
  // TODO: After beam release, use the latest client lib version which supports
  // setting lock
  // hints via the read api. SQL string generation should be removed.
  public static Statement generateReadSQLWithExclusiveLock(
      String tableName, List<String> readColumnList, Key primaryKey, Ddl ddl) {

    Dialect dialect = ddl.dialect();
    boolean isPostgres = dialect == Dialect.POSTGRESQL;
    Table table = ddl.table(tableName); // Lookup once
    List<IndexColumn> pks = table.primaryKeys();

    // Quote selected columns for Postgres
    String columnNames =
        isPostgres
            ? readColumnList.stream()
                .map(c -> quoteIdentifier(c, dialect))
                .collect(Collectors.joining(", "))
            : String.join(", ", readColumnList);

    // Generate WHERE clause
    String whereClause;
    if (isPostgres) {
      whereClause =
          IntStream.range(0, pks.size())
              .mapToObj(i -> quoteIdentifier(pks.get(i).name(), dialect) + "=$" + (i + 1))
              .collect(Collectors.joining(" AND "));
    } else {
      whereClause =
          pks.stream()
              .map(col -> col.name() + "=@" + col.name())
              .collect(Collectors.joining(" AND "));
    }

    // Build final SQL string
    String sql =
        (isPostgres ? "/*@ LOCK_SCANNED_RANGES=exclusive */ " : "@{LOCK_SCANNED_RANGES=exclusive} ")
            + "SELECT "
            + columnNames
            + " FROM "
            + quoteIdentifier(tableName, dialect)
            + " WHERE "
            + whereClause;

    Statement.Builder stmtBuilder = Statement.newBuilder(sql);
    int i = 0;
    for (Object value : primaryKey.getParts()) {
      String colName = pks.get(i).name();
      Type keyColType = table.column(colName).type();

      String bindName = isPostgres ? "p" + (i + 1) : colName;
      if (isPostgres) {
        bindPgValue(stmtBuilder, bindName, keyColType, value);
      } else {
        bindGoogleSqlValue(stmtBuilder, bindName, keyColType, value);
      }

      i++;
    }
    return stmtBuilder.build();
  }

  private static void bindGoogleSqlValue(
      Statement.Builder stmtBuilder, String bindName, Type type, Object value) {
    // TODO: Handle json type as PKs.
    switch (type.getCode()) {
      case BOOL:
        stmtBuilder.bind(bindName).to((Boolean) value);
        break;
      case INT64:
        stmtBuilder.bind(bindName).to((Long) value);
        break;
      case FLOAT64:
        stmtBuilder.bind(bindName).to((Double) value);
        break;
      case FLOAT32:
        stmtBuilder.bind(bindName).to((Float) value);
        break;
      case STRING:
      case JSON:
        stmtBuilder.bind(bindName).to((String) value);
        break;
      case NUMERIC:
        stmtBuilder.bind(bindName).to((BigDecimal) value);
        break;
      case BYTES:
        stmtBuilder.bind(bindName).to((ByteArray) value);
        break;
      case TIMESTAMP:
        /* Spanner TIMESTAMP type stores data in UTC. The com.google.cloud.Timestamp object
         * represents a point in time in UTC. Timezone information is not stored in Spanner
         * and is not retained by the Timestamp object itself, but the instant in time is safe.
         */
        stmtBuilder.bind(bindName).to((Timestamp) value);
        break;
      case DATE:
        stmtBuilder.bind(bindName).to((Date) value);
        break;
      default:
        throw new IllegalArgumentException("Unsupported Google SQL type: " + type);
    }
  }

  private static void bindPgValue(
      Statement.Builder stmtBuilder, String bindName, Type type, Object value) {
    // TODO: Handle json type as PKs.
    switch (type.getCode()) {
      case PG_BOOL:
        stmtBuilder.bind(bindName).to((Boolean) value);
        break;
      case PG_INT8:
        stmtBuilder.bind(bindName).to((Long) value);
        break;
      case PG_FLOAT8:
        stmtBuilder.bind(bindName).to((Double) value);
        break;
      case PG_FLOAT4:
        stmtBuilder.bind(bindName).to((Float) value);
        break;
      case PG_VARCHAR:
      case PG_TEXT:
      case PG_JSONB:
        stmtBuilder.bind(bindName).to((String) value);
        break;
      case PG_NUMERIC:
        stmtBuilder.bind(bindName).to(Value.pgNumeric(value.toString()));
        break;
      case PG_BYTEA:
        stmtBuilder.bind(bindName).to((ByteArray) value);
        break;
      case PG_COMMIT_TIMESTAMP:
      case PG_TIMESTAMPTZ:
        /* Spanner TIMESTAMP type stores data in UTC. The com.google.cloud.Timestamp object
         * represents a point in time in UTC. Timezone information is not stored in Spanner
         * and is not retained by the Timestamp object itself, but the instant in time is safe.
         */
        stmtBuilder.bind(bindName).to((Timestamp) value);
        break;
      case PG_DATE:
        stmtBuilder.bind(bindName).to((Date) value);
        break;
      default:
        throw new IllegalArgumentException("Unsupported PG type: " + type);
    }
  }

  private static String quoteIdentifier(String name, Dialect dialect) {
    return (dialect == Dialect.POSTGRESQL) ? "\"" + name + "\"" : name;
  }
}
