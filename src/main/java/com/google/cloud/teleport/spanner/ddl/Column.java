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
package com.google.cloud.teleport.spanner.ddl;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nullable;

/** Cloud Spanner column. */
@AutoValue
public abstract class Column implements Serializable {

  private static final long serialVersionUID = -1752579370892365181L;

  public abstract String name();

  public abstract Type type();

  public abstract ImmutableList<String> columnOptions();

  @Nullable
  public abstract Integer size();

  public abstract boolean notNull();

  public abstract boolean isGenerated();

  public abstract String generationExpression();

  public abstract boolean isStored();

  public abstract Dialect dialect();

  @Nullable
  public abstract String defaultExpression();

  public static Builder builder(Dialect dialect) {
    return new AutoValue_Column.Builder()
        .dialect(dialect)
        .columnOptions(ImmutableList.of())
        .notNull(false)
        .isGenerated(false)
        .generationExpression("")
        .isStored(false);
  }

  public static Builder builder() {
    return builder(Dialect.GOOGLE_STANDARD_SQL);
  }

  public void prettyPrint(Appendable appendable) throws IOException {
    if (dialect() != Dialect.GOOGLE_STANDARD_SQL && dialect() != Dialect.POSTGRESQL) {
      throw new IllegalArgumentException(String.format("Unrecognized Dialect: %s.", dialect()));
    }
    String identifierQuote = DdlUtilityComponents.identifierQuote(dialect());
    appendable
        .append(String.format("%1$-40s", identifierQuote + name() + identifierQuote))
        .append(typeString());
    if (notNull()) {
      appendable.append(" NOT NULL");
    }
    if ((dialect() == Dialect.POSTGRESQL) && defaultExpression() != null) {
      appendable.append(" DEFAULT ").append(defaultExpression());
    }
    if (isGenerated()) {
      if (dialect() == Dialect.POSTGRESQL) {
        appendable.append(" GENERATED ALWAYS");
      }
      appendable.append(" AS (").append(generationExpression()).append(")");
      if (isStored()) {
        appendable.append(" STORED");
      }
    }
    if (columnOptions() == null) {
      return;
    }
    String optionsString = String.join(",", columnOptions());
    if (!optionsString.isEmpty()) {
      appendable.append(" OPTIONS (").append(optionsString).append(")");
    }
  }

  public String prettyPrint() {
    StringBuilder sb = new StringBuilder();
    try {
      prettyPrint(sb);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return sb.toString();
  }

  public abstract Builder toBuilder();

  @Override
  public String toString() {
    return prettyPrint();
  }

  public String typeString() {
    return typeString(type(), size());
  }

  private static String typeString(Type type, Integer size) {
    switch (type.getCode()) {
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
        return "STRING(" + (size == -1 ? "MAX" : Integer.toString(size)) + ")";
      case PG_VARCHAR:
        return "character varying" + (size == -1 ? "" : ("(" + Integer.toString(size) + ")"));
      case PG_TEXT:
        return "text";
      case BYTES:
        return "BYTES(" + (size == -1 ? "MAX" : Integer.toString(size)) + ")";
      case PG_BYTEA:
        return "bytea";
      case DATE:
        return "DATE";
      case PG_DATE:
        return "date";
      case TIMESTAMP:
        return "TIMESTAMP";
      case PG_TIMESTAMPTZ:
        return "timestamp with time zone";
      case NUMERIC:
        return "NUMERIC";
      case PG_NUMERIC:
        return "numeric";
      case JSON:
        return "JSON";
      case PG_JSONB:
        return "jsonb";
      case ARRAY:
        {
          Type arrayType = type.getArrayElementType();
          return "ARRAY<" + typeString(arrayType, size) + ">";
        }
      case PG_ARRAY:
        {
          Type arrayType = type.getArrayElementType();
          return typeString(arrayType, size) + "[]";
        }
    }

    throw new IllegalArgumentException("Unknown type " + type);
  }

  /** A builder for {@link Column}. */
  @AutoValue.Builder
  public abstract static class Builder {

    private Table.Builder tableBuilder;

    Builder tableBuilder(Table.Builder tableBuilder) {
      this.tableBuilder = tableBuilder;
      return this;
    }

    abstract Builder name(String name);

    public abstract Builder type(Type type);

    public abstract Builder size(Integer size);

    public abstract Builder notNull(boolean nullable);

    abstract Builder dialect(Dialect dialect);

    abstract Dialect dialect();

    public Builder notNull() {
      return notNull(true);
    }

    public abstract Builder isGenerated(boolean generated);

    public abstract Builder generationExpression(String expression);

    public abstract Builder defaultExpression(String expression);

    public Builder generatedAs(String expression) {
      return isGenerated(true).generationExpression(expression);
    }

    public abstract Builder isStored(boolean generated);

    public Builder stored() {
      return isStored(true);
    }

    public abstract Column autoBuild();

    public Builder int64() {
      return type(Type.int64());
    }

    public Builder pgInt8() {
      return type(Type.pgInt8());
    }

    public Builder float64() {
      return type(Type.float64());
    }

    public Builder pgFloat8() {
      return type(Type.pgFloat8());
    }

    public Builder bool() {
      return type(Type.bool());
    }

    public Builder pgBool() {
      return type(Type.pgBool());
    }

    public Builder string() {
      return type(Type.string());
    }

    public Builder pgVarchar() {
      return type(Type.pgVarchar()).max();
    }

    public Builder pgText() {
      return type(Type.pgText()).max();
    }

    public Builder bytes() {
      return type(Type.bytes());
    }

    public Builder pgBytea() {
      return type(Type.pgBytea()).max();
    }

    public Builder timestamp() {
      return type(Type.timestamp());
    }

    public Builder pgTimestamptz() {
      return type(Type.pgTimestamptz());
    }

    public Builder date() {
      return type(Type.date());
    }

    public Builder pgDate() {
      return type(Type.pgDate());
    }

    public Builder numeric() {
      return type(Type.numeric());
    }

    public Builder pgNumeric() {
      return type(Type.pgNumeric());
    }

    public Builder json() {
      return type(Type.json());
    }

    public Builder pgJsonb() {
      return type(Type.pgJsonb());
    }

    public Builder max() {
      return size(-1);
    }

    public Builder parseType(String spannerType) {
      SizedType sizedType = parseSpannerType(spannerType, dialect());
      return type(sizedType.type).size(sizedType.size);
    }

    public Table.Builder endColumn() {
      tableBuilder.addColumn(this.autoBuild());
      return tableBuilder;
    }

    public abstract Builder columnOptions(ImmutableList<String> options);
  }

  private static class SizedType {

    public final Type type;
    public final Integer size;

    public SizedType(Type type, Integer size) {
      this.type = type;
      this.size = size;
    }
  }

  private static SizedType t(Type type, Integer size) {
    return new SizedType(type, size);
  }

  private static SizedType parseSpannerType(String spannerType, Dialect dialect) {
    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        {
          if (spannerType.equals("BOOL")) {
            return t(Type.bool(), null);
          }
          if (spannerType.equals("INT64")) {
            return t(Type.int64(), null);
          }
          if (spannerType.equals("FLOAT64")) {
            return t(Type.float64(), null);
          }
          if (spannerType.startsWith("STRING")) {
            String sizeStr = spannerType.substring(7, spannerType.length() - 1);
            int size = sizeStr.equals("MAX") ? -1 : Integer.parseInt(sizeStr);
            return t(Type.string(), size);
          }
          if (spannerType.startsWith("BYTES")) {
            String sizeStr = spannerType.substring(6, spannerType.length() - 1);
            int size = sizeStr.equals("MAX") ? -1 : Integer.parseInt(sizeStr);
            return t(Type.bytes(), size);
          }
          if (spannerType.equals("TIMESTAMP")) {
            return t(Type.timestamp(), null);
          }
          if (spannerType.equals("DATE")) {
            return t(Type.date(), null);
          }
          if (spannerType.equals("NUMERIC")) {
            return t(Type.numeric(), null);
          }
          if (spannerType.equals("JSON")) {
            return t(Type.json(), null);
          }
          if (spannerType.startsWith("ARRAY")) {
            // Substring "ARRAY<xxx>"
            String spannerArrayType = spannerType.substring(6, spannerType.length() - 1);
            SizedType itemType = parseSpannerType(spannerArrayType, dialect);
            return t(Type.array(itemType.type), itemType.size);
          }
          break;
        }
      case POSTGRESQL:
        {
          if (spannerType.endsWith("[]")) {
            // Substring "xxx[]"
            // Must check array type first
            String spannerArrayType = spannerType.substring(0, spannerType.length() - 2);
            SizedType itemType = parseSpannerType(spannerArrayType, dialect);
            return t(Type.pgArray(itemType.type), itemType.size);
          }
          if (spannerType.equals("boolean")) {
            return t(Type.pgBool(), null);
          }
          if (spannerType.equals("bigint")) {
            return t(Type.pgInt8(), null);
          }
          if (spannerType.equals("double precision")) {
            return t(Type.pgFloat8(), null);
          }
          if (spannerType.equals("text")) {
            return t(Type.pgText(), -1);
          }
          if (spannerType.startsWith("character varying")) {
            int size = -1;
            if (spannerType.length() > 18) {
              String sizeStr = spannerType.substring(18, spannerType.length() - 1);
              size = Integer.parseInt(sizeStr);
            }
            return t(Type.pgVarchar(), size);
          }
          if (spannerType.equals("bytea")) {
            return t(Type.pgBytea(), -1);
          }
          if (spannerType.equals("timestamp with time zone")) {
            return t(Type.pgTimestamptz(), null);
          }
          if (spannerType.equals("numeric")) {
            return t(Type.pgNumeric(), null);
          }
          if (spannerType.equals("jsonb")) {
            return t(Type.pgJsonb(), null);
          }
          if (spannerType.equals("date")) {
            return t(Type.pgDate(), null);
          }
          break;
        }
      default:
        break;
    }
    throw new IllegalArgumentException("Unknown spanner type " + spannerType);
  }
}
