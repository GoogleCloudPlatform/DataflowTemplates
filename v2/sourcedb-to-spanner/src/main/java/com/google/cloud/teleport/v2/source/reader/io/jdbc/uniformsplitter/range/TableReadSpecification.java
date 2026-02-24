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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.io.jdbc.JdbcIO;

/**
 * Defines the specification for reading from a table, including its identifier, row mapper, and
 * fetch size.
 *
 * @param <T> the type of the elements to be read.
 */
@AutoValue
public abstract class TableReadSpecification<T> implements Serializable {

  /**
   * Returns the identifier for the table.
   *
   * @return the table identifier.
   */
  public abstract TableIdentifier tableIdentifier();

  /**
   * Returns the row mapper for the table.
   *
   * @return the row mapper.
   */
  public abstract JdbcIO.RowMapper<T> rowMapper();

  /**
   * Returns the fetch size for the table.
   *
   * @return the fetch size.
   */
  public abstract Integer fetchSize();

  /**
   * Creates a builder for {@link TableReadSpecification}.
   *
   * @param <T> the type of the elements to be read.
   * @return a new builder instance.
   */
  public static <T> Builder<T> builder() {
    return new AutoValue_TableReadSpecification.Builder<T>().setFetchSize(50000);
  }

  @AutoValue.Builder
  public abstract static class Builder<T> {
    public abstract Builder<T> setTableIdentifier(TableIdentifier value);

    public abstract Builder<T> setRowMapper(JdbcIO.RowMapper<T> value);

    public abstract Builder<T> setFetchSize(Integer value);

    public abstract TableReadSpecification<T> build();
  }
}
