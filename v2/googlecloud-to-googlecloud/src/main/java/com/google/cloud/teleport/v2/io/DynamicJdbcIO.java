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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.DataSourceConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DynamicJdbcIO provides a {@link DynamicRead} class to allow templatized pipelines to read data
 * from a JDBC datasource using a driver class that is provided at runtime.
 *
 * <p>Since all possible JDBC drivers are not known at template creation time, the driver jars
 * cannot be prestaged in the template.
 *
 * <p>In order to provide the flexibility for the users to specify any JDBC driver class, the {@link
 * DynamicRead} and the {@link DynamicDataSourceConfiguration} classes allow the user to provide the
 * GCS path(s) for the JDBC driver jars.
 *
 * <p>These jars are then localized on the worker machines in the {@link DynamicReadFn#setup()}
 * method of the {@link DoFn}.
 *
 * <p>A new {@link URLClassLoader} is then created with the localized JDBC jar urls in the {@link
 * DynamicDataSourceConfiguration#buildDatasource()} method. The new {@link URLClassLoader} is added
 * to the {@link BasicDataSource#setDriverClassLoader(ClassLoader)}
 *
 * <p>The core functionality is a reproduction of {@link JdbcIO.Read} with the exception of the
 * dynamic class loading in the {@link DoFn}
 */
public class DynamicJdbcIO {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicJdbcIO.class);

  public static <T> DynamicRead<T> read() {
    return new AutoValue_DynamicJdbcIO_DynamicRead.Builder<T>().build();
  }

  public static <T> DynamicWrite<T> write() {
    return new AutoValue_DynamicJdbcIO_DynamicWrite.Builder<T>().build();
  }

  /** Implementation of {@link #read()}. */
  @AutoValue
  public abstract static class DynamicRead<T> extends PTransform<PBegin, PCollection<T>> {
    @Nullable
    abstract DynamicDataSourceConfiguration getDynamicDataSourceConfiguration();

    @Nullable
    abstract String getQuery();

    @Nullable
    abstract JdbcIO.RowMapper<T> getRowMapper();

    @Nullable
    abstract Coder<T> getCoder();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDynamicDataSourceConfiguration(DynamicDataSourceConfiguration config);

      abstract Builder<T> setQuery(String query);

      abstract Builder<T> setRowMapper(JdbcIO.RowMapper<T> rowMapper);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract DynamicRead<T> build();
    }

    public DynamicRead<T> withDataSourceConfiguration(
        DynamicDataSourceConfiguration configuration) {
      checkArgument(
          configuration != null,
          "withDataSourceConfiguration(configuration) called with null configuration");
      return toBuilder().setDynamicDataSourceConfiguration(configuration).build();
    }

    public DynamicRead<T> withQuery(String query) {
      checkArgument(query != null, "withQuery(query) called with null query");
      return toBuilder().setQuery(query).build();
    }

    public DynamicRead<T> withRowMapper(JdbcIO.RowMapper<T> rowMapper) {
      checkArgument(rowMapper != null, ".withRowMapper(rowMapper) called with null rowMapper");
      return toBuilder().setRowMapper(rowMapper).build();
    }

    public DynamicRead<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "withCoder(coder) called with null coder");
      return toBuilder().setCoder(coder).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      return input
          .apply(Create.of((Void) null))
          .apply(
              ParDo.of(
                  new DynamicReadFn<>(
                      getDynamicDataSourceConfiguration(), getQuery(), getRowMapper())))
          .setCoder(getCoder())
          .apply(new Reparallelize<>());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("query", getQuery()));
      builder.add(DisplayData.item("rowMapper", getRowMapper().getClass().getName()));
      builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
    }
  }

  /** Implementation of {@link #write()}. */
  @AutoValue
  public abstract static class DynamicWrite<T>
      extends PTransform<PCollection<T>, PCollection<FailsafeElement<T, T>>> {
    @Nullable
    abstract DynamicDataSourceConfiguration getDynamicDataSourceConfiguration();

    @Nullable
    abstract String getStatement();

    @Nullable
    abstract JdbcIO.PreparedStatementSetter<T> getPreparedStatementSetter();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDynamicDataSourceConfiguration(DynamicDataSourceConfiguration config);

      abstract Builder<T> setStatement(String statement);

      abstract Builder<T> setPreparedStatementSetter(
          JdbcIO.PreparedStatementSetter<T> preparedStatementSetter);

      abstract DynamicWrite<T> build();
    }

    public DynamicWrite<T> withDataSourceConfiguration(
        DynamicDataSourceConfiguration configuration) {
      checkArgument(
          configuration != null,
          "withDataSourceConfiguration(configuration) called with null configuration");
      return toBuilder().setDynamicDataSourceConfiguration(configuration).build();
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
                  getDynamicDataSourceConfiguration(),
                  getStatement(),
                  getPreparedStatementSetter())));
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

  /**
   * A POJO describing a {@link DataSource}, by providing all properties allowing to create a {@link
   * DataSource}.
   */
  @AutoValue
  public abstract static class DynamicDataSourceConfiguration implements Serializable {
    @Nullable
    abstract String getDriverClassName();

    @Nullable
    abstract String getUrl();

    @Nullable
    abstract String getUsername();

    @Nullable
    abstract String getPassword();

    @Nullable
    abstract String getConnectionProperties();

    @Nullable
    abstract String getDriverJars();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDriverClassName(String driverClassName);

      abstract Builder setUrl(String url);

      abstract Builder setUsername(String username);

      abstract Builder setPassword(String password);

      abstract Builder setConnectionProperties(String connectionProperties);

      abstract Builder setDriverJars(String jars);

      abstract DynamicDataSourceConfiguration build();
    }

    public static DynamicDataSourceConfiguration create(String driverClassName, String url) {
      checkArgument(driverClassName != null, "driverClassName can not be null");
      checkArgument(url != null, "url can not be null");
      return new AutoValue_DynamicJdbcIO_DynamicDataSourceConfiguration.Builder()
          .setDriverClassName(driverClassName)
          .setUrl(url)
          .build();
    }

    public DynamicDataSourceConfiguration withUsername(String username) {
      return builder().setUsername(username).build();
    }

    public DynamicDataSourceConfiguration withPassword(String password) {
      return builder().setPassword(password).build();
    }

    public DynamicDataSourceConfiguration withDriverJars(String driverJars) {
      checkArgument(
          driverJars != null, "withDriverJars(gcsPathStrings) called with null gcsPathStrings");
      return builder().setDriverJars(driverJars).build();
    }

    public DynamicDataSourceConfiguration withConnectionProperties(String connectionProperties) {
      checkArgument(connectionProperties != null, "connectionProperties can not be null");
      return builder().setConnectionProperties(connectionProperties).build();
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      builder.addIfNotNull(DisplayData.item("jdbcDriverClassName", getDriverClassName()));
      builder.addIfNotNull(DisplayData.item("jdbcUrl", getUrl()));
      builder.addIfNotNull(DisplayData.item("username", getUsername()));
      builder.addIfNotNull(DisplayData.item("driverJars", getDriverJars()));
    }

    @VisibleForTesting
    public DataSource buildDatasource() {
      BasicDataSource basicDataSource = new BasicDataSource();
      if (getDriverClassName() == null) {
        throw new RuntimeException("Driver class name is required.");
      } else {
        basicDataSource.setDriverClassName(getDriverClassName());
      }
      if (getUrl() == null) {
        throw new RuntimeException("Connection url is required.");
      }
      basicDataSource.setUrl(getUrl());
      if (getUsername() != null) {
        basicDataSource.setUsername(getUsername());
      }
      if (getPassword() != null) {
        basicDataSource.setPassword(getPassword());
      }
      if (getConnectionProperties() != null) {
        basicDataSource.setConnectionProperties(getConnectionProperties());
      }

      // Since the jdbc connection connection class might have dependencies that are not available
      // to the template, we will localize the user provided dependencies from GCS and create a new
      // {@link URLClassLoader} that is added to the {@link
      // BasicDataSource#setDriverClassLoader(ClassLoader)}
      if (getDriverJars() != null) {
        ClassLoader urlClassLoader = getNewClassLoader(getDriverJars());
        basicDataSource.setDriverClassLoader(urlClassLoader);
      }

      // wrapping the datasource as a pooling datasource
      DataSourceConnectionFactory connectionFactory =
          new DataSourceConnectionFactory(basicDataSource);
      PoolableConnectionFactory poolableConnectionFactory =
          new PoolableConnectionFactory(connectionFactory, null);
      GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
      poolConfig.setMaxTotal(1);
      poolConfig.setMinIdle(0);
      poolConfig.setMinEvictableIdleTimeMillis(10000);
      poolConfig.setSoftMinEvictableIdleTimeMillis(30000);
      GenericObjectPool connectionPool =
          new GenericObjectPool(poolableConnectionFactory, poolConfig);
      poolableConnectionFactory.setPool(connectionPool);
      poolableConnectionFactory.setDefaultAutoCommit(false);
      poolableConnectionFactory.setDefaultReadOnly(false);
      PoolingDataSource poolingDataSource = new PoolingDataSource(connectionPool);
      return poolingDataSource;
    }

    /**
     * Utility method that copies each of the user provided JDBC JAR to the worker and creates a new
     * URLClassLoader pointing to the URLs for the localized JDBC dependencies.
     */
    private static URLClassLoader getNewClassLoader(String paths) {
      List<String> listOfJarPaths = Splitter.on(',').trimResults().splitToList(paths);

      final String destRoot = Files.createTempDir().getAbsolutePath();
      URL[] urls =
          listOfJarPaths.stream()
              .map(
                  jarPath -> {
                    try {
                      ResourceId sourceResourceId = FileSystems.matchNewResource(jarPath, false);
                      File destFile = Paths.get(destRoot, sourceResourceId.getFilename()).toFile();
                      ResourceId destResourceId =
                          FileSystems.matchNewResource(destFile.getAbsolutePath(), false);
                      copy(sourceResourceId, destResourceId);
                      LOG.info(
                          "Localized jar: "
                              + sourceResourceId.toString()
                              + " to: "
                              + destResourceId.toString());

                      return destFile.toURI().toURL();
                    } catch (IOException ex) {
                      throw new RuntimeException(ex);
                    }
                  })
              .toArray(URL[]::new);

      return URLClassLoader.newInstance(urls);
    }

    /** utility method to copy binary (jar file) data from source to dest. */
    private static void copy(ResourceId source, ResourceId dest) throws IOException {
      try (ReadableByteChannel rbc = FileSystems.open(source)) {
        try (WritableByteChannel wbc = FileSystems.create(dest, MimeTypes.BINARY)) {
          ByteStreams.copy(rbc, wbc);
        }
      }
    }
  }

  /** A {@link DoFn} executing the SQL query to read from the database. */
  private static class DynamicReadFn<X, T> extends DoFn<X, T> {

    private final DynamicDataSourceConfiguration dataSourceConfiguration;
    private final String query;
    private final JdbcIO.RowMapper<T> rowMapper;

    private DataSource dataSource;
    private Connection connection;

    private DynamicReadFn(
        DynamicDataSourceConfiguration dataSourceConfiguration,
        String query,
        JdbcIO.RowMapper<T> rowMapper) {
      this.dataSourceConfiguration = dataSourceConfiguration;
      this.query = query;
      this.rowMapper = rowMapper;
    }

    @Setup
    public void setup() throws Exception {
      dataSource = dataSourceConfiguration.buildDatasource();
      connection = dataSource.getConnection();
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      try (PreparedStatement statement = connection.prepareStatement(query)) {
        try (ResultSet resultSet = statement.executeQuery()) {
          while (resultSet.next()) {
            context.output(rowMapper.mapRow(resultSet));
          }
        }
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

  /** A {@link DoFn} executing the SQL query to write to the database. */
  private static class DynamicWriteFn<T> extends DoFn<T, FailsafeElement<T, T>> {

    private final DynamicDataSourceConfiguration dataSourceConfiguration;
    private final String statement;
    private final JdbcIO.PreparedStatementSetter<T> preparedStatementSetter;

    private DataSource dataSource;
    private Connection connection;
    private PreparedStatement preparedStatement;

    private DynamicWriteFn(
        DynamicDataSourceConfiguration dataSourceConfiguration,
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

  private static class Reparallelize<T> extends PTransform<PCollection<T>, PCollection<T>> {
    @Override
    public PCollection<T> expand(PCollection<T> input) {
      // See https://issues.apache.org/jira/browse/BEAM-2803
      // We use a combined approach to "break fusion" here:
      // (see https://cloud.google.com/dataflow/service/dataflow-service-desc#preventing-fusion)
      // 1) force the data to be materialized by passing it as a side input to an identity fn,
      // then 2) reshuffle it with a random key. Initial materialization provides some parallelism
      // and ensures that data to be shuffled can be generated in parallel, while reshuffling
      // provides perfect parallelism.
      // In most cases where a "fusion break" is needed, a simple reshuffle would be sufficient.
      // The current approach is necessary only to support the particular case of JdbcIO where
      // a single query may produce many gigabytes of query results.
      PCollectionView<Iterable<T>> empty =
          input
              .apply("Consume", Filter.by(SerializableFunctions.constant(false)))
              .apply(View.asIterable());
      PCollection<T> materialized =
          input.apply(
              "Identity",
              ParDo.of(
                      new DoFn<T, T>() {
                        @ProcessElement
                        public void process(ProcessContext c) {
                          c.output(c.element());
                        }
                      })
                  .withSideInputs(empty));
      return materialized.apply(Reshuffle.viaRandomKey());
    }
  }
}
