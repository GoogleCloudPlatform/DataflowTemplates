/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.spanner;

import static com.google.cloud.teleport.spanner.AvroUtil.unpackNullable;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.common.NumericUtils;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.ddl.ChangeStream;
import com.google.cloud.teleport.spanner.ddl.Column;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.Table;
import com.google.cloud.teleport.spanner.ddl.View;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Allows to convert a set of Avro schemas to {@link Ddl}. */
public class AvroSchemaToDdlConverter {
  private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaToDdlConverter.class);
  private final Dialect dialect;

  public AvroSchemaToDdlConverter() {
    this.dialect = Dialect.GOOGLE_STANDARD_SQL;
  }

  public AvroSchemaToDdlConverter(Dialect dialect) {
    this.dialect = dialect;
  }

  public Ddl toDdl(Collection<Schema> avroSchemas) {
    Ddl.Builder builder = Ddl.builder(dialect);
    for (Schema schema : avroSchemas) {
      if (schema.getProp("spannerViewQuery") != null) {
        builder.addView(toView(null, schema));
      } else if (schema.getProp(AvroUtil.CHANGE_STREAM_FOR_CLAUSE) != null) {
        builder.addChangeStream(toChangeStream(null, schema));
      } else {
        builder.addTable(toTable(null, schema));
      }
    }
    return builder.build();
  }

  public View toView(String viewName, Schema schema) {
    if (viewName == null) {
      viewName = schema.getName();
    }
    LOG.debug("Converting to Ddl viewName {}", viewName);

    View.Builder builder =
        View.builder(dialect).name(viewName).query(schema.getProp("spannerViewQuery"));
    if (schema.getProp("spannerViewSecurity") != null) {
      builder.security(View.SqlSecurity.valueOf(schema.getProp("spannerViewSecurity")));
    }
    return builder.build();
  }

  public ChangeStream toChangeStream(String changeStreamName, Schema schema) {
    if (changeStreamName == null) {
      changeStreamName = schema.getName();
    }
    LOG.debug("Converting to Ddl changeStreamName {}", changeStreamName);

    ChangeStream.Builder builder =
        ChangeStream.builder()
            .name(changeStreamName)
            .forClause(schema.getProp(AvroUtil.CHANGE_STREAM_FOR_CLAUSE));

    ImmutableList.Builder<String> changeStreamOptions = ImmutableList.builder();
    for (int i = 0; ; i++) {
      String spannerOption = schema.getProp("spannerOption_" + i);
      if (spannerOption == null) {
        break;
      }
      changeStreamOptions.add(spannerOption);
    }
    builder.options(changeStreamOptions.build());

    return builder.build();
  }

  public Table toTable(String tableName, Schema schema) {
    if (tableName == null) {
      tableName = schema.getName();
    }
    LOG.debug("Converting to Ddl tableName {}", tableName);

    Table.Builder table = Table.builder(dialect);
    table.name(tableName);
    for (Schema.Field f : schema.getFields()) {
      Column.Builder column = table.column(f.name());
      String sqlType = f.getProp("sqlType");
      String expression = f.getProp("generationExpression");
      if (expression != null) {
        // This is a generated column.
        if (Strings.isNullOrEmpty(sqlType)) {
          throw new IllegalArgumentException(
              "Property sqlType is missing for generated column " + f.name());
        }
        String notNull = f.getProp("notNull");
        if (notNull == null) {
          throw new IllegalArgumentException(
              "Property notNull is missing for generated column " + f.name());
        }
        column.parseType(sqlType).notNull(Boolean.parseBoolean(notNull)).generatedAs(expression);
        String stored = f.getProp("stored");
        if (stored == null) {
          throw new IllegalArgumentException(
              "Property stored is missing for generated column " + f.name());
        }
        if (Boolean.parseBoolean(stored)) {
          column.stored();
        }
      } else {
        boolean nullable = false;
        Schema avroType = f.schema();
        if (avroType.getType() == Schema.Type.UNION) {
          Schema unpacked = unpackNullable(avroType);
          nullable = unpacked != null;
          if (nullable) {
            avroType = unpacked;
          }
        }
        if (Strings.isNullOrEmpty(sqlType)) {
          Type spannerType = inferType(avroType, true);
          sqlType = toString(spannerType, true);
        }
        String defaultExpression = f.getProp("defaultExpression");
        column.parseType(sqlType).notNull(!nullable).defaultExpression(defaultExpression);
      }
      ImmutableList.Builder<String> columnOptions = ImmutableList.builder();
      for (int i = 0; ; i++) {
        String spannerOption = f.getProp("spannerOption_" + i);
        if (spannerOption == null) {
          break;
        }
        columnOptions.add(spannerOption);
      }
      column.columnOptions(columnOptions.build());
      column.endColumn();
    }

    for (int i = 0; ; i++) {
      String spannerPrimaryKey = schema.getProp("spannerPrimaryKey_" + i);
      if (spannerPrimaryKey == null) {
        break;
      }
      if (spannerPrimaryKey.endsWith(" ASC")) {
        String name = spannerPrimaryKey.substring(0, spannerPrimaryKey.length() - 4);
        table.primaryKey().asc(unescape(name, dialect)).end();
      } else if (spannerPrimaryKey.endsWith(" DESC")) {
        String name = spannerPrimaryKey.substring(0, spannerPrimaryKey.length() - 5);
        table.primaryKey().desc(unescape(name, dialect)).end();
      } else {
        throw new IllegalArgumentException("Cannot parse spannerPrimaryKey " + spannerPrimaryKey);
      }
    }

    table.indexes(getNumberedPropsWithPrefix(schema, "spannerIndex_"));

    table.foreignKeys(getNumberedPropsWithPrefix(schema, "spannerForeignKey_"));

    table.checkConstraints(getNumberedPropsWithPrefix(schema, "spannerCheckConstraint_"));

    // Table parent options.
    String spannerParent = schema.getProp("spannerParent");
    if (!Strings.isNullOrEmpty(spannerParent)) {
      table.interleaveInParent(spannerParent);

      // Process the on delete action.
      String onDeleteAction = schema.getProp("spannerOnDeleteAction");
      if (onDeleteAction == null) {
        // Preserve behavior for old versions of exporter that did not provide the
        // spannerOnDeleteAction property.
        onDeleteAction = "no action";
      }
      if (onDeleteAction.equals("cascade")) {
        table.onDeleteCascade();
      } else if (!onDeleteAction.equals("no action")) {
        // This is an unknown on delete action.
        throw new IllegalArgumentException("Unsupported ON DELETE action " + onDeleteAction);
      }
    }

    return table.build();
  }

  // TODO: maybe encapsulate in the Ddl library.
  private static String unescape(String name, Dialect dialect) {
    if ((dialect == Dialect.GOOGLE_STANDARD_SQL && name.startsWith("`") && name.endsWith("`"))
        || (dialect == Dialect.POSTGRESQL && name.startsWith("\"") && name.endsWith("\""))) {
      return name.substring(1, name.length() - 1);
    }
    return name;
  }

  private static ImmutableList<String> getNumberedPropsWithPrefix(Schema schema, String prefix) {
    ImmutableList.Builder<String> props = ImmutableList.builder();
    for (int i = 0; ; i++) {
      String prop = schema.getProp(prefix + i);
      if (prop == null) {
        break;
      }
      props.add(prop);
    }
    return props.build();
  }

  private com.google.cloud.teleport.spanner.common.Type inferType(Schema f, boolean supportArrays) {
    Schema.Type type = f.getType();
    LogicalType logicalType = LogicalTypes.fromSchema(f);

    switch (type) {
      case BOOLEAN:
        return (dialect == Dialect.GOOGLE_STANDARD_SQL)
            ? com.google.cloud.teleport.spanner.common.Type.bool()
            : com.google.cloud.teleport.spanner.common.Type.pgBool();
      case INT:
        return (dialect == Dialect.GOOGLE_STANDARD_SQL)
            ? com.google.cloud.teleport.spanner.common.Type.int64()
            : com.google.cloud.teleport.spanner.common.Type.pgInt8();
      case LONG:
        if (LogicalTypes.timestampMillis().equals(logicalType)
            || LogicalTypes.timestampMicros().equals(logicalType)) {
          return (dialect == Dialect.GOOGLE_STANDARD_SQL)
              ? com.google.cloud.teleport.spanner.common.Type.timestamp()
              : com.google.cloud.teleport.spanner.common.Type.pgTimestamptz();
        }
        return (dialect == Dialect.GOOGLE_STANDARD_SQL)
            ? com.google.cloud.teleport.spanner.common.Type.int64()
            : com.google.cloud.teleport.spanner.common.Type.pgInt8();
      case FLOAT:
      case DOUBLE:
        return (dialect == Dialect.GOOGLE_STANDARD_SQL)
            ? com.google.cloud.teleport.spanner.common.Type.float64()
            : com.google.cloud.teleport.spanner.common.Type.pgFloat8();
      case STRING:
        return (dialect == Dialect.GOOGLE_STANDARD_SQL)
            ? com.google.cloud.teleport.spanner.common.Type.string()
            : com.google.cloud.teleport.spanner.common.Type.pgVarchar();
      case BYTES:
        if (LogicalTypes.decimal(NumericUtils.PRECISION, NumericUtils.SCALE).equals(logicalType)
            && dialect == Dialect.GOOGLE_STANDARD_SQL) {
          return com.google.cloud.teleport.spanner.common.Type.numeric();
        }
        if (LogicalTypes.decimal(NumericUtils.PG_MAX_PRECISION, NumericUtils.PG_MAX_SCALE)
                .equals(logicalType)
            && dialect == Dialect.POSTGRESQL) {
          return com.google.cloud.teleport.spanner.common.Type.pgNumeric();
        }
        return (dialect == Dialect.GOOGLE_STANDARD_SQL)
            ? com.google.cloud.teleport.spanner.common.Type.bytes()
            : com.google.cloud.teleport.spanner.common.Type.pgBytea();
      case ARRAY:
        {
          if (supportArrays) {
            Schema element = f.getElementType();
            if (element.getType() == Schema.Type.UNION) {
              Schema unpacked = unpackNullable(element);
              if (unpacked == null) {
                throw new IllegalArgumentException("Cannot infer a type " + f);
              }
              element = unpacked;
            }
            try {
              return (dialect == Dialect.GOOGLE_STANDARD_SQL)
                  ? com.google.cloud.teleport.spanner.common.Type.array(inferType(element, false))
                  : com.google.cloud.teleport.spanner.common.Type.pgArray(
                      inferType(element, false));
            } catch (IllegalArgumentException e) {
              throw new IllegalArgumentException("Cannot infer array type for field " + f);
            }
          }
          // Throw exception.
          break;
        }
    }
    throw new IllegalArgumentException("Cannot infer a type " + f);
  }

  private String toString(
      com.google.cloud.teleport.spanner.common.Type spannerType, boolean supportArray) {
    switch (spannerType.getCode()) {
      case BOOL:
        return "BOOL";
      case PG_BOOL:
        return "boolean";
      case INT64:
        return "INT64";
      case PG_INT8:
        return "bigint";
      case FLOAT64:
        return "FLOAT64";
      case PG_FLOAT8:
        return "double precision";
      case STRING:
        return "STRING(MAX)";
      case PG_TEXT:
        return "text";
      case PG_VARCHAR:
        return "character varying";
      case BYTES:
        return "BYTES(MAX)";
      case PG_BYTEA:
        return "bytea";
      case TIMESTAMP:
        return "TIMESTAMP";
      case PG_TIMESTAMPTZ:
        return "timestamp with time zone";
      case DATE:
        return "DATE";
      case PG_DATE:
        return "date";
      case NUMERIC:
        return "NUMERIC";
      case PG_NUMERIC:
        return "numeric";
      case JSON:
        return "JSON";
      case ARRAY:
        {
          if (supportArray) {
            com.google.cloud.teleport.spanner.common.Type element =
                spannerType.getArrayElementType();
            String elementStr = toString(element, false);
            return "ARRAY<" + elementStr + ">";
          }
          // otherwise fall through and throw an error.
          break;
        }
      case PG_ARRAY:
        {
          if (supportArray) {
            com.google.cloud.teleport.spanner.common.Type element =
                spannerType.getArrayElementType();
            String elementStr = toString(element, false);
            return elementStr + "[]";
          }
          // otherwise fall through and throw an error.
          break;
        }
    }
    throw new IllegalArgumentException("Cannot to string the type " + spannerType);
  }
}
