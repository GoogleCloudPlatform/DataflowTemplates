/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.io;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Throwables;
import java.sql.Connection;
import java.sql.PreparedStatement;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DynamicJdbcIO provides a {@link DynamicWrite} class to allow template pipelines to write data to
 * a JDBC datasource and outputs failed writes as FailsafeElement.
 */
public class DynamicJdbcIO {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicJdbcIO.class);

  public static <T> DynamicWrite<T> write() {
    return new AutoValue_DynamicJdbcIO_DynamicWrite.Builder<T>().build();
  }

  /** Implementation of {@link #write()}. */
  @AutoValue
  public abstract static class DynamicWrite<T>
      extends PTransform<PCollection<T>, PCollection<FailsafeElement<T, T>>> {
    @Nullable
    abstract JdbcIO.DataSourceConfiguration getDataSourceConfiguration();

    @Nullable
    abstract String getStatement();

    @Nullable
    abstract JdbcIO.PreparedStatementSetter<T> getPreparedStatementSetter();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceConfiguration(JdbcIO.DataSourceConfiguration config);

      abstract Builder<T> setStatement(String statement);

      abstract Builder<T> setPreparedStatementSetter(
          JdbcIO.PreparedStatementSetter<T> preparedStatementSetter);

      abstract DynamicWrite<T> build();
    }

    public DynamicWrite<T> withDataSourceConfiguration(
        JdbcIO.DataSourceConfiguration configuration) {
      checkArgument(
          configuration != null,
          "withDataSourceConfiguration(configuration) called with null configuration");
      return toBuilder().setDataSourceConfiguration(configuration).build();
    }

    public DynamicWrite<T> withStatement(String statement) {
      checkArgument(statement != null, "withStatement(statement) called with null statement");
      return toBuilder().setStatement(statement).build();
    }

    public DynamicWrite<T> withPreparedStatementSetter(
        JdbcIO.PreparedStatementSetter<T> preparedStatementSetter) {
      checkArgument(
          preparedStatementSetter != null,
          "withPreparedStatementSetter(preparedStatementSetter) called with null"
              + " preparedStatementSetter");
      return toBuilder().setPreparedStatementSetter(preparedStatementSetter).build();
    }

    @Override
    public PCollection<FailsafeElement<T, T>> expand(PCollection<T> input) {
      return input.apply(
          ParDo.of(
              new DynamicWriteFn<>(
                  getDataSourceConfiguration(), getStatement(), getPreparedStatementSetter())));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("statement", getStatement()));
      builder.add(
          DisplayData.item(
              "preparedStatementSetter", getPreparedStatementSetter().getClass().getName()));
    }
  }

  /** A {@link DoFn} executing the SQL query to write to the database. */
  private static class DynamicWriteFn<T> extends DoFn<T, FailsafeElement<T, T>> {

    private final JdbcIO.DataSourceConfiguration dataSourceConfiguration;
    private final String statement;
    private final JdbcIO.PreparedStatementSetter<T> preparedStatementSetter;

    private DataSource dataSource;
    private Connection connection;
    private PreparedStatement preparedStatement;

    private DynamicWriteFn(
        JdbcIO.DataSourceConfiguration dataSourceConfiguration,
        String statement,
        JdbcIO.PreparedStatementSetter<T> preparedStatementSetter) {
      this.dataSourceConfiguration = dataSourceConfiguration;
      this.statement = statement;
      this.preparedStatementSetter = preparedStatementSetter;
    }

    @Setup
    public void setup() throws Exception {
      dataSource = dataSourceConfiguration.buildDatasource();
      connection = dataSource.getConnection();
      connection.setAutoCommit(false);
      preparedStatement = connection.prepareStatement(statement);
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      try {
        preparedStatement.clearParameters();
        preparedStatementSetter.setParameters(context.element(), preparedStatement);
        preparedStatement.execute();
        connection.commit();
      } catch (Exception e) {
        LOG.error("Error while executing statement: {}", e.getMessage());
        context.output(
            FailsafeElement.of(context.element(), context.element())
                .setErrorMessage(e.getMessage())
                .setStacktrace(Throwables.getStackTraceAsString(e)));
      }
    }

    @Teardown
    public void teardown() throws Exception {
      connection.close();
      if (dataSource instanceof AutoCloseable) {
        ((AutoCloseable) dataSource).close();
      }
    }
  }
}
