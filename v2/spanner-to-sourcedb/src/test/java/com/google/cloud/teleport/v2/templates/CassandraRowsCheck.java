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
package com.google.cloud.teleport.v2.templates;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.it.conditions.ConditionCheck;

@AutoValue
public abstract class CassandraRowsCheck extends ConditionCheck {

  abstract CassandraResourceManager resourceManager();

  abstract String tableName();

  abstract Integer minRows();

  @Nullable
  abstract Integer maxRows();

  @Override
  public String getDescription() {
    if (maxRows() != null) {
      return String.format(
          "Cassandra table check if table %s has between %d and %d rows",
          tableName(), minRows(), maxRows());
    }
    return String.format(
        "Cassandra table check if table %s has at least %d rows", tableName(), minRows());
  }

  private long getRowCount(String tableName) {
    String query = String.format("SELECT COUNT(*) FROM %s", tableName);
    ResultSet resultSet = resourceManager().executeStatement(query, 10);
    Row row = resultSet.one();
    if (row != null) {
      return row.getLong(0);
    } else {
      throw new RuntimeException("Query did not return a result for table: " + tableName);
    }
  }

  @Override
  public CheckResult check() {
    long totalRows = getRowCount(tableName());
    if (totalRows < minRows()) {
      return new CheckResult(
          false,
          String.format("Expected at least %d rows but found only %d", minRows(), totalRows));
    }
    if (maxRows() != null && totalRows > maxRows()) {
      return new CheckResult(
          false, String.format("Expected up to %d rows but found %d", maxRows(), totalRows));
    }

    if (maxRows() != null) {
      return new CheckResult(
          true,
          String.format(
              "Expected between %d and %d rows and found %d", minRows(), maxRows(), totalRows));
    }

    return new CheckResult(
        true, String.format("Expected at least %d rows and found %d", minRows(), totalRows));
  }

  public static Builder builder(CassandraResourceManager resourceManager, String tableName) {
    return new AutoValue_CassandraRowsCheck.Builder()
        .setResourceManager(resourceManager)
        .setTableName(tableName);
  }

  /** Builder for {@link CassandraRowsCheck}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setResourceManager(CassandraResourceManager resourceManager);

    public abstract Builder setTableName(String tableName);

    public abstract Builder setMinRows(Integer minRows);

    public abstract Builder setMaxRows(Integer maxRows);

    abstract CassandraRowsCheck autoBuild();

    public CassandraRowsCheck build() {
      return autoBuild();
    }
  }
}
