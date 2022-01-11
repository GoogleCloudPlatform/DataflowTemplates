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
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;

/** A Cloud Spanner index column. */
@AutoValue
public abstract class IndexColumn implements Serializable {

  private static final long serialVersionUID = -976796114694704550L;

  public abstract String name();

  public abstract Order order();

  public static IndexColumn create(String name, Order order) {
    return new AutoValue_IndexColumn(name, order);
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

  public void prettyPrint(Appendable appendable) throws IOException {
    appendable.append("`").append(name()).append("` ").append(order().title);
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
  public static class IndexColumnsBuilder<T> {
    private ImmutableList.Builder<IndexColumn> columns = ImmutableList.builder();

    private T callback;

    public IndexColumnsBuilder(T callback) {
      this.callback = callback;
    }

    public IndexColumnsBuilder<T> asc(String name) {
      IndexColumn indexColumn = IndexColumn.create(name, Order.ASC);
      return set(indexColumn);
    }

    public IndexColumnsBuilder<T> set(IndexColumn indexColumn) {
      columns.add(indexColumn);
      return this;
    }

    public IndexColumnsBuilder<T> desc(String name) {
      return set(IndexColumn.create(name, Order.DESC));
    }

    public IndexColumnsBuilder<T> storing(String name) {
      return set(IndexColumn.create(name, Order.STORING));
    }

    public ImmutableList<IndexColumn> build() {
      return columns.build();
    }

    public T end() {
      return callback;
    }
  }
}
