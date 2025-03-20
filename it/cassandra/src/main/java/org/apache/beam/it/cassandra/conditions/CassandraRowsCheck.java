/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.it.cassandra.conditions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.google.auto.value.AutoValue;
import java.net.InetSocketAddress;
import java.time.Duration;
import javax.annotation.Nullable;
import org.apache.beam.it.cassandra.CassandraResourceManager;
import org.apache.beam.it.conditions.ConditionCheck;

/**
 * Condition check for verifying the number of rows in a Cassandra table. This class is generic,
 * allowing any type of Cassandra resource manager to be used at runtime.
 */
@AutoValue
public abstract class CassandraRowsCheck extends ConditionCheck {

  @Nullable
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

  /**
   * Gets the row count for the specified table using the given CassandraResourceManager.
   *
   * @param resourceManager The CassandraResourceManager to use for the query.
   * @param tableName The name of the table to count rows from.
   * @return The number of rows in the table.
   */
  private long getRowCount(CassandraResourceManager resourceManager, String tableName) {
    if (resourceManager == null) {
      throw new IllegalArgumentException("CassandraResourceManager must not be null.");
    }
    try (CqlSession session =
        CqlSession.builder()
            .addContactPoint(
                new InetSocketAddress(resourceManager.getHost(), resourceManager.getPort()))
            .withLocalDatacenter("datacenter1")
            .build()) {

      String query =
          String.format("SELECT COUNT(*) FROM %s.%s", resourceManager.getKeyspaceName(), tableName);
      SimpleStatement statement =
          SimpleStatement.builder(query).setTimeout(Duration.ofSeconds(20)).build();
      ResultSet resultSet = session.execute(statement);
      Row row = resultSet.one();
      if (row != null) {
        return row.getLong(0);
      } else {
        throw new RuntimeException("Query did not return a result for table: " + tableName);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to execute query on CassandraResourceManager", e);
    }
  }

  @Override
  public CheckResult check() {
    long totalRows = getRowCount(resourceManager(), tableName());
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

  /**
   * Builder for {@link CassandraRowsCheck}. Now allows setting the CassandraResourceManager at
   * runtime.
   */
  public static Builder builder(String tableName) {
    return new AutoValue_CassandraRowsCheck.Builder().setTableName(tableName);
  }

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
