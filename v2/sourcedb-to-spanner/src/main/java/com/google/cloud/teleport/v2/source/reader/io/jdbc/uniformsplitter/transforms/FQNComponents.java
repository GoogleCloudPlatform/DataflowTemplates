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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to fail-safely parse and store Fully Qualified Name (FQN) components from JDBC
 * metadata.
 *
 * <p>These components are used to report accurate data lineage to the Dataflow service, enabling
 * users to visualize the data flow from specific source tables/databases to their destinations.
 */
@AutoValue
public abstract class FQNComponents {

  static final String DEFAULT_SCHEMA = "default";

  abstract String getScheme();

  abstract Iterable<String> getSegments();

  private static final Logger LOG = LoggerFactory.getLogger(FQNComponents.class);

  /**
   * Reports lineage to the provided {@link Lineage} object.
   *
   * @param lineage the lineage object to report to.
   * @param tableWithSchema a {@link KV} containing (schema, table) names.
   */
  void reportLineage(Lineage lineage, @Nullable KV<String, String> tableWithSchema) {
    ImmutableList.Builder<String> builder = ImmutableList.<String>builder().addAll(getSegments());
    if (tableWithSchema != null) {
      if (tableWithSchema.getKey() != null && !tableWithSchema.getKey().isEmpty()) {
        builder.add(tableWithSchema.getKey());
      } else {
        // Every database engine has the default schema or search path if user hasn't provided
        // one. The name
        // is specific to db engine. For PostgreSQL it is public, for MSSQL it is dbo.
        // Users can have custom default scheme for the benefit of the user but dataflow is unable
        // to determine that.
        builder.add(DEFAULT_SCHEMA);
      }
      if (!tableWithSchema.getValue().isEmpty()) {
        builder.add(tableWithSchema.getValue());
      }
    }
    lineage.add(getScheme(), builder.build());
  }

  /**
   * Fail-safely extract FQN from supported {@link DataSource}.
   *
   * @param dataSource the data source to extract from.
   * @return FQNComponents or null if extraction fails.
   */
  static @Nullable FQNComponents of(DataSource dataSource) {
    // Supported case CloudSql using HikariDataSource
    // Had to retrieve properties via Reflection to avoid introduce mandatory Hikari dependencies
    String maybeSqlInstance;
    String url;
    try {
      if (dataSource instanceof BasicDataSource) {
        // try default data source implementation
        BasicDataSource source = (BasicDataSource) dataSource;
        Method getProperties = source.getClass().getDeclaredMethod("getConnectionProperties");
        getProperties.setAccessible(true);
        Properties properties = (Properties) getProperties.invoke(dataSource);
        if (properties == null) {
          return null;
        }
        maybeSqlInstance = properties.getProperty("cloudSqlInstance");
        if (maybeSqlInstance == null) {
          // not a cloudSqlInstance
          return null;
        }
        url = source.getUrl();
      } else { // try recommended as per best practice
        Class<?> hikariClass = Class.forName("com.zaxxer.hikari.HikariDataSource");
        if (!hikariClass.isInstance(dataSource)) {
          return null;
        }
        Method getProperties = hikariClass.getMethod("getDataSourceProperties");
        Properties properties = (Properties) getProperties.invoke(dataSource);
        if (properties == null) {
          return null;
        }
        maybeSqlInstance = properties.getProperty("cloudSqlInstance");
        if (maybeSqlInstance == null) {
          // not a cloudSqlInstance
          return null;
        }
        Method getUrl = hikariClass.getMethod("getJdbcUrl");
        url = (String) getUrl.invoke(dataSource);
        if (url == null) {
          return null;
        }
      }
    } catch (ClassNotFoundException
        | InvocationTargetException
        | IllegalAccessException
        | NoSuchMethodException e) {
      return null;
    }

    JdbcUrl jdbcUrl = JdbcUrl.of(url);
    if (jdbcUrl == null) {
      LOG.info("Failed to parse JdbcUrl {}. Lineage will not be reported.", url);
      return null;
    }

    String scheme = "cloudsql_" + jdbcUrl.getScheme();
    ImmutableList.Builder<String> segments = ImmutableList.builder();
    List<String> sqlInstance = Arrays.asList(maybeSqlInstance.split(":"));
    if (sqlInstance.size() > 3) {
      // project name contains ":"
      segments
          .add(String.join(":", sqlInstance.subList(0, sqlInstance.size() - 2)))
          .add(sqlInstance.get(sqlInstance.size() - 2))
          .add(sqlInstance.get(sqlInstance.size() - 1));
    } else {
      segments.addAll(Arrays.asList(maybeSqlInstance.split(":")));
    }
    segments.add(jdbcUrl.getDatabase());
    return new AutoValue_FQNComponents(scheme, segments.build());
  }

  /**
   * Fail-safely extract FQN from an active {@link Connection}.
   *
   * @param connection the connection to extract from.
   * @return FQNComponents or null if extraction fails.
   */
  static @Nullable FQNComponents of(Connection connection) {
    try {
      DatabaseMetaData metadata = connection.getMetaData();
      if (metadata == null) {
        // usually not-null, but can be null when running a mock
        return null;
      }
      String url = metadata.getURL();
      if (url == null) {
        // usually not-null, but can be null when running a mock
        return null;
      }
      return of(url);
    } catch (Exception e) {
      // suppressed
      return null;
    }
  }

  /**
   * Fail-safely parse FQN from a Jdbc URL. Return null if failed.
   *
   * <p>e.g.
   *
   * <p>jdbc:postgresql://localhost:5432/postgres -> (postgresql, [localhost:5432, postgres])
   *
   * <p>jdbc:mysql://127.0.0.1:3306/db -> (mysql, [127.0.0.1:3306, db])
   */
  @VisibleForTesting
  static @Nullable FQNComponents of(String url) {
    JdbcUrl jdbcUrl = JdbcUrl.of(url);
    if (jdbcUrl == null) {
      LOG.info("Failed to parse JdbcUrl {}. Lineage will not be reported.", url);
      return null;
    }
    String hostAndPort = jdbcUrl.getHostAndPort();
    if (hostAndPort == null) {
      LOG.info("Failed to parse host/port from JdbcUrl {}. Lineage will not be reported.", url);
      return null;
    }

    return new AutoValue_FQNComponents(
        jdbcUrl.getScheme(), ImmutableList.of(hostAndPort, jdbcUrl.getDatabase()));
  }
}
