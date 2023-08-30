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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.dbcp2.BasicDataSource;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO to write raw SQL using JDBC.
 *
 * <p>To configure the JDBC source, you have to provide a {@link DataSourceConfiguration} using<br>
 * 1. {@link DataSourceConfiguration#create(DataSource)}(which must be {@link Serializable});<br>
 * 2. or {@link DataSourceConfiguration#create(String, String)}(driver class name and url).
 * Optionally, {@link DataSourceConfiguration#withUsername(String)} and {@link
 * DataSourceConfiguration#withPassword(String)} allows you to define username and password.
 *
 * <h3>Writing to JDBC datasource</h3>
 *
 * <p>JDBC sink supports writing records into a database. It writes a {@link PCollection} to the
 * database by converting each T into a string via a user-provided {@link StatementFormatter}.
 *
 * <p>Like the source, to configure the sink, you have to provide a {@link DataSourceConfiguration}.
 *
 * <pre>{@code
 * pipeline
 *   .apply(...)
 *   .apply(JdbcIO.<KV<Integer, String>>write()
 *      .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
 *            "com.mysql.jdbc.Driver", "jdbc:mysql://hostname:3306/mydb")
 *          .withUsername("username")
 *          .withPassword("password"))
 *    );
 * }</pre>
 *
 * <p>NB: in case of transient failures, Beam runners may execute parts of JdbcIO.Write multiple
 * times for fault tolerance. Because of that, you should avoid using {@code INSERT} statements,
 * since that risks duplicating records in the database, or failing due to primary key conflicts.
 * Consider using <a href="https://en.wikipedia.org/wiki/Merge_(SQL)">MERGE ("upsert")
 * statements</a> supported by your database instead.
 */
public class CdcJdbcIO {

  private static final Logger LOG = LoggerFactory.getLogger(CdcJdbcIO.class);

  private static final long DEFAULT_BATCH_SIZE = 1000L;
  private static final int DEFAULT_FETCH_SIZE = 50_000;

  /**
   * Write data to a JDBC datasource.
   *
   * @param <T> Type of the data to be written.
   */
  public static <T> Write<T> write() {
    return new Write();
  }

  public static <T> WriteVoid<T> writeVoid() {
    return new AutoValue_CdcJdbcIO_WriteVoid.Builder<T>()
        .setBatchSize(DEFAULT_BATCH_SIZE)
        .setRetryStrategy(new DefaultRetryStrategy())
        .build();
  }

  /**
   * This is the default {@link Predicate} we use to detect DeadLock. It basically tests if the
   * {@link SQLException#getSQLState()} equals 40001. 40001 is the SQL State used by most databases
   * to identify a deadlock.
   */
  public static class DefaultRetryStrategy implements RetryStrategy {
    @Override
    public boolean apply(SQLException e) {
      return "40001".equals(e.getSQLState());
    }
  }

  private CdcJdbcIO() {}

  /**
   * A POJO describing a {@link DataSource}, either providing directly a {@link DataSource} or all
   * properties allowing to create a {@link DataSource}.
   */
  @AutoValue
  public abstract static class DataSourceConfiguration implements Serializable {
    @Nullable
    abstract ValueProvider<String> getDriverClassName();

    @Nullable
    abstract ValueProvider<String> getUrl();

    @Nullable
    abstract ValueProvider<String> getUsername();

    @Nullable
    abstract ValueProvider<String> getPassword();

    @Nullable
    abstract ValueProvider<Integer> getMaxIdleConnections();

    @Nullable
    abstract ValueProvider<String> getConnectionProperties();

    @Nullable
    abstract ValueProvider<Collection<String>> getConnectionInitSqls();

    @Nullable
    abstract DataSource getDataSource();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDriverClassName(ValueProvider<String> driverClassName);

      abstract Builder setUrl(ValueProvider<String> url);

      abstract Builder setUsername(ValueProvider<String> username);

      abstract Builder setPassword(ValueProvider<String> password);

      abstract Builder setMaxIdleConnections(ValueProvider<Integer> maxIdleConnections);

      abstract Builder setConnectionProperties(ValueProvider<String> connectionProperties);

      abstract Builder setConnectionInitSqls(ValueProvider<Collection<String>> connectionInitSqls);

      abstract Builder setDataSource(DataSource dataSource);

      abstract DataSourceConfiguration build();
    }

    public static DataSourceConfiguration create(DataSource dataSource) {
      checkArgument(dataSource != null, "dataSource can not be null");
      checkArgument(dataSource instanceof Serializable, "dataSource must be Serializable");
      return new AutoValue_CdcJdbcIO_DataSourceConfiguration.Builder()
          .setDataSource(dataSource)
          .build();
    }

    public static DataSourceConfiguration create(String driverClassName, String url) {
      checkArgument(driverClassName != null, "driverClassName can not be null");
      checkArgument(url != null, "url can not be null");
      return create(
          ValueProvider.StaticValueProvider.of(driverClassName),
          ValueProvider.StaticValueProvider.of(url));
    }

    public static DataSourceConfiguration create(
        ValueProvider<String> driverClassName, ValueProvider<String> url) {
      checkArgument(driverClassName != null, "driverClassName can not be null");
      checkArgument(url != null, "url can not be null");
      return new AutoValue_CdcJdbcIO_DataSourceConfiguration.Builder()
          .setDriverClassName(driverClassName)
          .setUrl(url)
          .build();
    }

    public DataSourceConfiguration withUsername(String username) {
      return withUsername(ValueProvider.StaticValueProvider.of(username));
    }

    public DataSourceConfiguration withUsername(ValueProvider<String> username) {
      return builder().setUsername(username).build();
    }

    public DataSourceConfiguration withPassword(String password) {
      return withPassword(ValueProvider.StaticValueProvider.of(password));
    }

    public DataSourceConfiguration withPassword(ValueProvider<String> password) {
      return builder().setPassword(password).build();
    }

    public DataSourceConfiguration withMaxIdleConnections(Integer maxIdleConnections) {
      return withMaxIdleConnections(ValueProvider.StaticValueProvider.of(maxIdleConnections));
    }

    public DataSourceConfiguration withMaxIdleConnections(
        ValueProvider<Integer> maxIdleConnections) {
      return builder().setMaxIdleConnections(maxIdleConnections).build();
    }

    /**
     * Sets the connection properties passed to driver.connect(...). Format of the string must be
     * [propertyName=property;]*
     *
     * <p>NOTE - The "user" and "password" properties can be add via {@link #withUsername(String)},
     * {@link #withPassword(String)}, so they do not need to be included here.
     */
    public DataSourceConfiguration withConnectionProperties(String connectionProperties) {
      checkArgument(connectionProperties != null, "connectionProperties can not be null");
      return withConnectionProperties(ValueProvider.StaticValueProvider.of(connectionProperties));
    }

    /** Same as {@link #withConnectionProperties(String)} but accepting a ValueProvider. */
    public DataSourceConfiguration withConnectionProperties(
        ValueProvider<String> connectionProperties) {
      checkArgument(connectionProperties != null, "connectionProperties can not be null");
      return builder().setConnectionProperties(connectionProperties).build();
    }

    /**
     * Sets the connection init sql statements to driver.connect(...).
     *
     * <p>NOTE - This property is not applicable across databases. Only MySQL and MariaDB support
     * this. A Sql exception is thrown if your database does not support it.
     */
    public DataSourceConfiguration withConnectionInitSqls(Collection<String> connectionInitSqls) {
      checkArgument(connectionInitSqls != null, "connectionInitSqls can not be null");
      return withConnectionInitSqls(ValueProvider.StaticValueProvider.of(connectionInitSqls));
    }

    /** Same as {@link #withConnectionInitSqls(Collection)} but accepting a ValueProvider. */
    public DataSourceConfiguration withConnectionInitSqls(
        ValueProvider<Collection<String>> connectionInitSqls) {
      checkArgument(connectionInitSqls != null, "connectionInitSqls can not be null");
      checkArgument(!connectionInitSqls.get().isEmpty(), "connectionInitSqls can not be empty");
      return builder().setConnectionInitSqls(connectionInitSqls).build();
    }

    void populateDisplayData(DisplayData.Builder builder) {
      if (getDataSource() != null) {
        builder.addIfNotNull(DisplayData.item("dataSource", getDataSource().getClass().getName()));
      } else {
        builder.addIfNotNull(DisplayData.item("jdbcDriverClassName", getDriverClassName()));
        builder.addIfNotNull(DisplayData.item("jdbcUrl", getUrl()));
        builder.addIfNotNull(DisplayData.item("username", getUsername()));
      }
    }

    public DataSource buildDatasource() {
      if (getDataSource() == null) {
        BasicDataSource basicDataSource = new BasicDataSource();
        if (getDriverClassName() != null) {
          basicDataSource.setDriverClassName(getDriverClassName().get());
        }
        if (getUrl() != null) {
          basicDataSource.setUrl(getUrl().get());
        }
        if (getUsername() != null) {
          basicDataSource.setUsername(getUsername().get());
        }
        if (getPassword() != null) {
          basicDataSource.setPassword(getPassword().get());
        }
        if (getConnectionProperties() != null && getConnectionProperties().get() != null) {
          basicDataSource.setConnectionProperties(getConnectionProperties().get());
        }
        if (getConnectionInitSqls() != null
            && getConnectionInitSqls().get() != null
            && !getConnectionInitSqls().get().isEmpty()) {
          basicDataSource.setConnectionInitSqls(getConnectionInitSqls().get());
        }
        if (getMaxIdleConnections() != null && getMaxIdleConnections().get() != null) {
          basicDataSource.setMaxIdle(getMaxIdleConnections().get().intValue());
        }

        return basicDataSource;
      }
      return getDataSource();
    }
  }

  /**
   * An interface used by the JdbcIO Write to set the parameters of the {@link PreparedStatement}
   * used to setParameters into the database.
   */
  @FunctionalInterface
  public interface StatementFormatter<T> extends Serializable {
    String formatStatement(T element);
  }

  /**
   * An interface used to control if we retry the statements when a {@link SQLException} occurs. If
   * {@link RetryStrategy#apply(SQLException)} returns true, {@link Write} tries to replay the
   * statements.
   */
  @FunctionalInterface
  public interface RetryStrategy extends Serializable {
    boolean apply(SQLException sqlException);
  }

  /**
   * This class is used as the default return value of {@link JdbcIO#write()}.
   *
   * <p>All methods in this class delegate to the appropriate method of {@link JdbcIO.WriteVoid}.
   */
  public static class Write<T> extends PTransform<PCollection<T>, PDone> {
    WriteVoid<T> inner;

    Write() {
      this(CdcJdbcIO.writeVoid());
    }

    Write(WriteVoid<T> inner) {
      this.inner = inner;
    }

    /** See {@link WriteVoid#withDataSourceConfiguration(DataSourceConfiguration)}. */
    public Write<T> withDataSourceConfiguration(DataSourceConfiguration config) {
      return new Write(inner.withDataSourceConfiguration(config));
    }

    /** See {@link WriteVoid#withDataSourceProviderFn(SerializableFunction)}. */
    public Write<T> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return new Write(inner.withDataSourceProviderFn(dataSourceProviderFn));
    }

    /** See {@link WriteVoid#withStatementFormatter(StatementFormatter)}. */
    public Write<T> withStatementFormatter(StatementFormatter<T> formatter) {
      return new Write(inner.withStatementFormatter(formatter));
    }

    /** See {@link WriteVoid#withBatchSize(long)}. */
    public Write<T> withBatchSize(long batchSize) {
      return new Write(inner.withBatchSize(batchSize));
    }

    /** See {@link WriteVoid#withRetryStrategy(RetryStrategy)}. */
    public Write<T> withRetryStrategy(RetryStrategy retryStrategy) {
      return new Write(inner.withRetryStrategy(retryStrategy));
    }

    /**
     * Returns {@link WriteVoid} transform which can be used in {@link Wait#on(PCollection[])} to
     * wait until all data is written.
     *
     * <p>Example: write a {@link PCollection} to one database and then to another database, making
     * sure that writing a window of data to the second database starts only after the respective
     * window has been fully written to the first database.
     *
     * <pre>{@code
     * PCollection<Void> firstWriteResults = data.apply(JdbcIO.write()
     *     .withDataSourceConfiguration(CONF_DB_1).withResults());
     * data.apply(Wait.on(firstWriteResults))
     *     .apply(JdbcIO.write().withDataSourceConfiguration(CONF_DB_2));
     * }</pre>
     */
    public WriteVoid<T> withResults() {
      return inner;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      inner.populateDisplayData(builder);
    }

    @Override
    public PDone expand(PCollection<T> input) {
      inner.expand(input);
      return PDone.in(input.getPipeline());
    }
  }

  /** A {@link PTransform} to write to a JDBC datasource. */
  @AutoValue
  public abstract static class WriteVoid<T> extends PTransform<PCollection<T>, PCollection<Void>> {
    @Nullable
    abstract SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    abstract long getBatchSize();

    @Nullable
    abstract StatementFormatter<T> getStatementFormatter();

    @Nullable
    abstract RetryStrategy getRetryStrategy();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<T> setBatchSize(long batchSize);

      abstract Builder<T> setStatementFormatter(StatementFormatter<T> formatter);

      abstract Builder<T> setRetryStrategy(RetryStrategy deadlockPredicate);

      abstract WriteVoid<T> build();
    }

    public WriteVoid<T> withDataSourceConfiguration(DataSourceConfiguration config) {
      return withDataSourceProviderFn(new DataSourceProviderFromDataSourceConfiguration(config));
    }

    public WriteVoid<T> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
    }

    public WriteVoid<T> withStatementFormatter(StatementFormatter<T> formatter) {
      return toBuilder().setStatementFormatter(formatter).build();
    }

    /**
     * Provide a maximum size in number of SQL statement for the batch. Default is 1000.
     *
     * @param batchSize maximum batch size in number of statements
     */
    public WriteVoid<T> withBatchSize(long batchSize) {
      checkArgument(batchSize > 0, "batchSize must be > 0, but was %s", batchSize);
      return toBuilder().setBatchSize(batchSize).build();
    }

    /**
     * When a SQL exception occurs, {@link Write} uses this {@link RetryStrategy} to determine if it
     * will retry the statements. If {@link RetryStrategy#apply(SQLException)} returns {@code true},
     * then {@link Write} retries the statements.
     */
    public WriteVoid<T> withRetryStrategy(RetryStrategy retryStrategy) {
      checkArgument(retryStrategy != null, "retryStrategy can not be null");
      return toBuilder().setRetryStrategy(retryStrategy).build();
    }

    @Override
    public PCollection<Void> expand(PCollection<T> input) {
      checkArgument(
          (getDataSourceProviderFn() != null),
          "withDataSourceConfiguration() or withDataSourceProviderFn() is required");

      return input.apply(ParDo.of(new WriteFn<>(this)));
    }

    private static class WriteFn<T> extends DoFn<T, Void> {

      private final WriteVoid<T> spec;

      private static final int MAX_RETRIES = 5;
      private static final FluentBackoff BUNDLE_WRITE_BACKOFF =
          FluentBackoff.DEFAULT
              .withMaxRetries(MAX_RETRIES)
              .withInitialBackoff(Duration.standardSeconds(5));

      private DataSource dataSource;
      private Connection connection;
      private Statement statement;
      private final List<T> records = new ArrayList<>();

      public WriteFn(WriteVoid<T> spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() {
        dataSource = spec.getDataSourceProviderFn().apply(null);
      }

      @StartBundle
      public void startBundle() throws Exception {
        connection = dataSource.getConnection();
        connection.setAutoCommit(false);
        statement = connection.createStatement();
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        T record = context.element();

        records.add(record);

        if (records.size() >= spec.getBatchSize()) {
          executeBatch();
        }
      }

      @FinishBundle
      public void finishBundle() throws Exception {
        executeBatch();
        try {
          if (statement != null) {
            statement.close();
          }
        } finally {
          if (connection != null) {
            connection.close();
          }
        }
      }

      private void executeBatch() throws SQLException, IOException, InterruptedException {
        if (records.isEmpty()) {
          return;
        }
        Sleeper sleeper = Sleeper.DEFAULT;
        BackOff backoff = BUNDLE_WRITE_BACKOFF.backoff();
        boolean singleStatementMode = false;
        while (true) {
          try {
            executeBatchStatementFormatting(singleStatementMode);
            break;
          } catch (SQLException exception) {
            // TODO: How should we handle failures?
            connection.rollback();
            if (!BackOffUtils.next(sleeper, backoff)) {
              if (singleStatementMode) {
                // max batch retries reached, running
                throw exception;
              }
              // attempt to execute one statement at a time as a final attempt
              singleStatementMode = true;
            }
            LOG.warn("SQLException Occurred: {}", exception.toString());
          }
        }
        records.clear();
      }

      private void executeBatchStatementFormatting(boolean singleStatementMode)
          throws SQLException, IOException, InterruptedException {
        if (singleStatementMode) {
          executeBatchSingleStatementFormatting();
        } else {
          executeBatchMultiStatementFormatting();
        }
      }

      private void executeBatchMultiStatementFormatting()
          throws SQLException, IOException, InterruptedException {
        statement = connection.createStatement();

        for (T record : records) {
          String formattedStatement = spec.getStatementFormatter().formatStatement(record);
          statement.addBatch(formattedStatement);
        }

        // execute the batch
        statement.executeBatch();
        connection.commit();
      }

      private void executeBatchSingleStatementFormatting()
          throws SQLException, IOException, InterruptedException {
        statement = connection.createStatement();

        for (T record : records) {
          String formattedStatement = spec.getStatementFormatter().formatStatement(record);
          try {
            statement.executeUpdate(formattedStatement);
            connection.commit();
          } catch (SQLException exception) {
            LOG.error("SQLException Occurred: {}", exception.toString());
            connection.rollback();
          }
        }
      }
    }
  }

  /**
   * Wraps a {@link DataSourceConfiguration} to provide a {@link DataSource}.
   *
   * <p>At most a single {@link DataSource} instance will be constructed during pipeline execution
   * for each unique {@link DataSourceConfiguration} within the pipeline.
   */
  public static class DataSourceProviderFromDataSourceConfiguration
      implements SerializableFunction<Void, DataSource>, HasDisplayData {
    private static final ConcurrentHashMap<DataSourceConfiguration, DataSource> instances =
        new ConcurrentHashMap<>();
    private final DataSourceConfiguration config;

    private DataSourceProviderFromDataSourceConfiguration(DataSourceConfiguration config) {
      this.config = config;
    }

    public static SerializableFunction<Void, DataSource> of(DataSourceConfiguration config) {
      return new DataSourceProviderFromDataSourceConfiguration(config);
    }

    @Override
    public DataSource apply(Void input) {
      return instances.computeIfAbsent(config, DataSourceConfiguration::buildDatasource);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      config.populateDisplayData(builder);
    }
  }
}
