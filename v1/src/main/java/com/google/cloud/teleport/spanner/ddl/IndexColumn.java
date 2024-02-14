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

import static com.google.cloud.teleport.spanner.common.DdlUtils.quoteIdentifier;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Dialect;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nullable;

/** A Cloud Spanner index column. */
@AutoValue
public abstract class IndexColumn implements Serializable {

  private static final long serialVersionUID = -976796114694704550L;

  public abstract String name();

  public abstract Order order();

  public abstract Dialect dialect();

  // restricted to PG
  @Nullable
  public abstract NullsOrder nullsOrder();

  public static IndexColumn create(String name, Order order, Dialect dialect) {
    return new AutoValue_IndexColumn.Builder().dialect(dialect).name(name).order(order).autoBuild();
  }

  public static IndexColumn create(String name, Order order) {
    return create(name, order, Dialect.GOOGLE_STANDARD_SQL);
  }

  /** Ordering of column in the index. */
  public enum Order {
    ASC("ASC"),
    DESC("DESC"),
    STORING("STORING");

    Order(String title) {
      this.title = title;
    }

    public static Order defaultOrder() {
      return ASC;
    }

    private final String title;
  }

  /** Ordering of null values in the column. */
  public enum NullsOrder {
    FIRST("FIRST"),
    LAST("LAST");

    NullsOrder(String title) {
      this.title = title;
    }

    private final String title;
  }

  public void prettyPrint(Appendable appendable) throws IOException {
    appendable
        .append(quoteIdentifier(name(), dialect()))
        .append(" ")
        .append(order().title);
    if (nullsOrder() != null) {
      appendable.append(" NULLS ").append(nullsOrder().title);
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

  @Override
  public String toString() {
    return prettyPrint();
  }

  /** A builder for {@link IndexColumn}. */
  @AutoValue.Builder
  public abstract static class Builder {

    abstract Builder name(String name);

    abstract Builder order(Order order);

    abstract Builder dialect(Dialect dialect);

    abstract Builder nullsOrder(NullsOrder nullsOrder);

    abstract IndexColumn autoBuild();
  }

  /** A builder for {@link IndexColumn}. */
  public static class IndexColumnsBuilder<T> {

    private ImmutableList.Builder<IndexColumn> columns = ImmutableList.builder();

    private Builder indexColumnBuilder;

    private T callback;

    private Dialect dialect;

    public IndexColumnsBuilder(T callback, Dialect dialect) {
      this.callback = callback;
      this.dialect = dialect;
    }

    public IndexColumnsBuilder<T> asc(String name) {
      IndexColumn indexColumn = IndexColumn.create(name, Order.ASC, dialect);
      return set(indexColumn);
    }

    public IndexColumnsBuilder<T> desc(String name) {
      return set(IndexColumn.create(name, Order.DESC, dialect));
    }

    public IndexColumnsBuilder<T> storing(String name) {
      return set(IndexColumn.create(name, Order.STORING, dialect));
    }

    IndexColumnsBuilder<T> set(IndexColumn indexColumn) {
      columns.add(indexColumn);
      return this;
    }

    public IndexColumnsBuilder<T> create() {
      indexColumnBuilder = new AutoValue_IndexColumn.Builder().dialect(dialect);
      return this;
    }

    public IndexColumnsBuilder<T> name(String name) {
      if (indexColumnBuilder == null) {
        throw new IllegalArgumentException(
            "Builder is missing. Call create method to initiate a builder first.");
      }
      indexColumnBuilder.name(name);
      return this;
    }

    public IndexColumnsBuilder<T> asc() {
      if (indexColumnBuilder == null) {
        throw new IllegalArgumentException(
            "Builder is missing. Call create method to initiate a builder first.");
      }
      indexColumnBuilder.order(Order.ASC);
      return this;
    }

    public IndexColumnsBuilder<T> desc() {
      if (indexColumnBuilder == null) {
        throw new IllegalArgumentException(
            "Builder is missing. Call create method to initiate a builder first.");
      }
      indexColumnBuilder.order(Order.DESC);
      return this;
    }

    public IndexColumnsBuilder<T> storing() {
      if (indexColumnBuilder == null) {
        throw new IllegalArgumentException(
            "Builder is missing. Call create method to initiate a builder first.");
      }
      indexColumnBuilder.order(Order.STORING);
      return this;
    }

    public IndexColumnsBuilder<T> nullsFirst() {
      if (indexColumnBuilder == null) {
        throw new IllegalArgumentException(
            "Builder is missing. Call create method to initiate a builder first.");
      }
      indexColumnBuilder.nullsOrder(NullsOrder.FIRST);
      return this;
    }

    public IndexColumnsBuilder<T> nullsLast() {
      if (indexColumnBuilder == null) {
        throw new IllegalArgumentException(
            "Builder is missing. Call create method to initiate a builder first.");
      }
      indexColumnBuilder.nullsOrder(NullsOrder.LAST);
      return this;
    }

    public ImmutableList<IndexColumn> build() {
      return columns.build();
    }

    public IndexColumnsBuilder<T> endIndexColumn() {
      set(indexColumnBuilder.autoBuild());
      indexColumnBuilder = null;
      return this;
    }

    public T end() {
      return callback;
    }
  }
}
