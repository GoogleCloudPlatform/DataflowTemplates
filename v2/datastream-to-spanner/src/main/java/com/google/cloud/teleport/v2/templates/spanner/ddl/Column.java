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
package com.google.cloud.teleport.v2.templates.spanner.ddl;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Type;
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

  public static Builder builder() {
    return new AutoValue_Column.Builder()
        .columnOptions(ImmutableList.of())
        .notNull(false)
        .isGenerated(false)
        .generationExpression("")
        .isStored(false);
  }

  public void prettyPrint(Appendable appendable) throws IOException {
    appendable.append(String.format("%1$-40s", "`" + name() + "`")).append(typeString());
    if (notNull()) {
      appendable.append(" NOT NULL");
    }
    if (isGenerated()) {
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
      case INT64:
        return "INT64";
      case FLOAT64:
        return "FLOAT64";
      case STRING:
        return "STRING(" + (size == -1 ? "MAX" : Integer.toString(size)) + ")";
      case BYTES:
        return "BYTES(" + (size == -1 ? "MAX" : Integer.toString(size)) + ")";
      case DATE:
        return "DATE";
      case TIMESTAMP:
        return "TIMESTAMP";
      case ARRAY:
        Type arrayType = type.getArrayElementType();
        return "ARRAY<" + typeString(arrayType, size) + ">";
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

    public Builder notNull() {
      return notNull(true);
    }

    public abstract Builder isGenerated(boolean generated);

    public abstract Builder generationExpression(String expression);

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

    public Builder float64() {

      return type(Type.float64());
    }

    public Builder bool() {
      return type(Type.bool());
    }

    public Builder string() {
      return type(Type.string());
    }

    public Builder bytes() {
      return type(Type.bytes());
    }

    public Builder timestamp() {
      return type(Type.timestamp());
    }

    public Builder date() {
      return type(Type.date());
    }

    public Builder max() {
      return size(-1);
    }

    public Builder parseType(String spannerType) {
      SizedType sizedType = parseSpannerType(spannerType);
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

  private static SizedType parseSpannerType(String spannerType) {
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
    if (spannerType.startsWith("ARRAY")) {
      // Substring "ARRAY<"xxx">"
      String spannerArrayType = spannerType.substring(6, spannerType.length() - 1);
      SizedType itemType = parseSpannerType(spannerArrayType);
      return t(Type.array(itemType.type), itemType.size);
    }
    throw new IllegalArgumentException("Unknown spanner type " + spannerType);
  }
}
