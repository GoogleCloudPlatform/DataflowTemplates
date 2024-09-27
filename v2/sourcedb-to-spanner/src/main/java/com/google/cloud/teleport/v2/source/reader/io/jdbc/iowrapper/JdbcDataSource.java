/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper;

import com.google.cloud.teleport.v2.source.reader.auth.dbauth.DbAuth;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.spanner.migrations.utils.JarFileReader;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.URLClassLoader;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.jline.utils.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Serializable} extension of {@link BasicDataSource}.
 *
 * <p>{@link org.apache.beam.sdk.io.jdbc.JdbcIO JdbcIO} provides 2 ways to register a {@lnk
 * DataSource} for reading:
 *
 * <ol>
 *   <li>Through {@link org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration
 *       DataSourceConfiguration} - This provides an extremely limited set of tunables like maximum
 *       connection in the pool.
 *   <li>By passing a serializable {@link javax.sql.DataSource}
 * </ol>
 *
 * For long lived connections, it's needed to tune the eviction times and idle times of the
 * connection pool. This needs us to pass a serializable datasource to {@link
 * org.apache.beam.sdk.io.jdbc.JdbcIO JdbcIO}. Since the native java {@link DataSource} is not
 * serializable, we extend the DataSource and serialize and deserialize the required properties that
 * we need to fine tune.
 *
 * @see <a
 *     href=https://commons.apache.org/proper/commons-dbcp/configuration.html>commons-dbcp/configuration</a>
 */
public final class JdbcDataSource extends BasicDataSource implements Serializable {

  /*
   * Implementation Detail, we take required members of JDBCIOWrapperConfig as private members here since there are members of JDBCIOWrapperConfig like FluentBackoff which aren't marked serializable by Apache Beam.
   */

  private final String sourceDbURL;
  private final DbAuth dbAuth;
  private final ImmutableList<String> initSql;
  private final Long maxConnections;
  private final String jdbcDriverJars;
  private final String jdbcDriverClassName;
  private static final Logger LOG = LoggerFactory.getLogger(JdbcDataSource.class);

  /**
   * Sets the {@code testOnBorrow} property. This property determines whether or not the pool will
   * validate objects before they are borrowed from the pool. Defaults to True.
   */
  private final Boolean testOnBorrow;

  /**
   * Sets the {@code testOnCreate} property. This property determines whether or not the pool will
   * validate objects immediately after they are created by the pool.
   */
  private final Boolean testOnCreate;

  /**
   * Sets the {@code testOnReturn} property. This property determines whether or not the pool will
   * validate objects before they are returned to the pool.
   */
  private final Boolean testOnReturn;

  /**
   * Sets the {@code testWhileIdle} property. This property determines whether or not the idle
   * object evictor will validate connections.
   */
  private final Boolean testWhileIdle;

  /** Sets the {@code validationQuery}. */
  private final String validationQuery;

  /**
   * The timeout in seconds before an abandoned connection can be removed.
   *
   * <p>Creating a Statement, PreparedStatement or CallableStatement or using one of these to
   * execute a query (using one of the execute methods) resets the lastUsed property of the parent
   * connection.
   *
   * <p>Abandoned connection cleanup happens when:
   *
   * <ul>
   *   <li>{@link BasicDataSource#getRemoveAbandonedOnBorrow()} or {@link
   *       BasicDataSource#getRemoveAbandonedOnMaintenance()} = true
   *   <li>{@link BasicDataSource#getNumIdle() numIdle} &lt; 2
   *   <li>{@link BasicDataSource#getNumActive() numActive} &gt; {@link
   *       BasicDataSource#getMaxTotal() maxTotal} - 3
   * </ul>
   *
   * <p>
   */
  private final Integer removeAbandonedTimeout;

  /**
   * The minimum amount of time an object may sit idle in the pool before it is eligible for
   * eviction by the idle object evictor.
   */
  private final Integer minEvictableIdleTimeMillis;

  public JdbcDataSource(JdbcIOWrapperConfig jdbcIOWrapperConfig) {
    this.sourceDbURL = jdbcIOWrapperConfig.sourceDbURL();
    this.dbAuth = jdbcIOWrapperConfig.dbAuth();
    this.initSql = jdbcIOWrapperConfig.sqlInitSeq();
    this.maxConnections = jdbcIOWrapperConfig.maxConnections();
    this.jdbcDriverClassName = jdbcIOWrapperConfig.jdbcDriverClassName();
    this.jdbcDriverJars = jdbcIOWrapperConfig.jdbcDriverJars();
    this.testOnBorrow = jdbcIOWrapperConfig.testOnBorrow();
    this.testOnCreate = jdbcIOWrapperConfig.testOnCreate();
    this.testOnReturn = jdbcIOWrapperConfig.testOnReturn();
    this.testWhileIdle = jdbcIOWrapperConfig.testWhileIdle();
    this.validationQuery = jdbcIOWrapperConfig.validationQuery();
    this.removeAbandonedTimeout = jdbcIOWrapperConfig.removeAbandonedTimeout();
    this.minEvictableIdleTimeMillis = jdbcIOWrapperConfig.minEvictableIdleTimeMillis();
    this.initializeSuper();
  }

  private void initializeSuper() {

    Log.info("Initializing {}", this);

    super.setDriverClassName(jdbcDriverClassName);

    /**
     * This has been tested in an end to end migration and can't be unit tested as the jars are not
     * provided in GCS for UT.
     */
    if (!StringUtils.isBlank(jdbcDriverJars)) {
      URLClassLoader classLoader =
          URLClassLoader.newInstance(JarFileReader.saveFilesLocally(jdbcDriverJars));
      super.setDriverClassLoader(classLoader);
    }
    super.setUrl(sourceDbURL);
    super.setUsername(dbAuth.getUserName().get());
    super.setPassword(dbAuth.getPassword().get());

    if (!initSql.isEmpty()) {
      super.setConnectionInitSqls(initSql);
    }
    if (maxConnections != null) {
      super.setMaxTotal(maxConnections.intValue());
    }
    super.setTestOnBorrow(testOnBorrow);
    super.setTestOnCreate(testOnCreate);
    super.setTestOnReturn(testOnReturn);
    super.setTestWhileIdle(testWhileIdle);
    /* TODO(vardhanvthigle): move this to config */
    super.setValidationQuery(validationQuery);
    super.setRemoveAbandonedTimeout(removeAbandonedTimeout);
    super.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    // Call initializeSuper after deserialization
    initializeSuper();
  }

  @Override
  public String toString() {
    return String.format(
        "JdbcDataSource: {\"sourceDbURL\":\"%s\", \"initSql\":\"%s\", \"maxConnections\",\"%s\" }",
        sourceDbURL, initSql, maxConnections);
  }
}
