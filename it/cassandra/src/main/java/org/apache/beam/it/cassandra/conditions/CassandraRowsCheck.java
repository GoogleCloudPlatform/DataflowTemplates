package org.apache.beam.it.cassandra.conditions;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.it.conditions.ConditionCheck;

/**
 * Condition check for verifying the number of rows in a Cassandra table. This class is generic,
 * allowing any type of Cassandra resource manager to be used at runtime.
 *
 * @param <T> Type of the Cassandra resource manager, must extend AutoCloseable.
 */
@AutoValue
public abstract class CassandraRowsCheck<T> extends ConditionCheck {

  @Nullable
  abstract T resourceManager();

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
  private long getRowCount(T resourceManager, String tableName) {
    if (resourceManager == null) {
      throw new IllegalArgumentException("CassandraResourceManager must not be null.");
    }

    // Use reflection to call executeStatement if it exists
    try {
      String query = String.format("SELECT COUNT(*) FROM %s", tableName);
      ResultSet resultSet =
          (ResultSet)
              resourceManager
                  .getClass()
                  .getMethod("executeStatement", String.class, int.class)
                  .invoke(resourceManager, query, 10);

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
   *
   * @param <T> Type of the CassandraResourceManager.
   */
  public static <T> Builder<T> builder(String tableName) {
    return new AutoValue_CassandraRowsCheck.Builder<T>().setTableName(tableName);
  }

  @AutoValue.Builder
  public abstract static class Builder<T> {

    public abstract Builder<T> setResourceManager(T resourceManager);

    public abstract Builder<T> setTableName(String tableName);

    public abstract Builder<T> setMinRows(Integer minRows);

    public abstract Builder<T> setMaxRows(Integer maxRows);

    abstract CassandraRowsCheck<T> autoBuild();

    public CassandraRowsCheck<T> build() {
      return autoBuild();
    }
  }
}
