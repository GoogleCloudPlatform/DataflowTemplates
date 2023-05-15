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
import com.google.cloud.teleport.spanner.common.SizedType;
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
    if (defaultExpression() != null) {
      appendable.append(" DEFAULT ");
      if (dialect() == Dialect.POSTGRESQL) {
        appendable.append(defaultExpression());
      } else {
        appendable.append(" (").append(defaultExpression()).append(")");
      }
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
    return SizedType.typeString(type(), size());
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

    public Builder pgSpannerCommitTimestamp() {
      return type(Type.pgSpannerCommitTimestamp());
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
      SizedType sizedType = SizedType.parseSpannerType(spannerType, dialect());
      return type(sizedType.type).size(sizedType.size);
    }

    public Table.Builder endColumn() {
      tableBuilder.addColumn(this.autoBuild());
      return tableBuilder;
    }

    public abstract Builder columnOptions(ImmutableList<String> options);
  }
}
