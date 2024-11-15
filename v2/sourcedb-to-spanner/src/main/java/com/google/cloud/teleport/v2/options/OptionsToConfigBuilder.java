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
package com.google.cloud.teleport.v2.options;

import static com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig.builderWithMySqlDefaults;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig.builderWithPostgreSQLDefaults;

import com.google.cloud.teleport.v2.source.reader.auth.dbauth.LocalCredentialsProvider;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.defaults.MySqlConfigDefaults;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map.Entry;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OptionsToConfigBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(OptionsToConfigBuilder.class);
  public static final String DEFAULT_POSTGRESQL_NAMESPACE = "public";

  public static JdbcIOWrapperConfig getJdbcIOWrapperConfigWithDefaults(
      SourceDbToSpannerOptions options,
      List<String> tables,
      String shardId,
      Wait.OnSignal<?> waitOn) {
    SQLDialect sqlDialect = SQLDialect.valueOf(options.getSourceDbDialect());
    String sourceDbURL = options.getSourceConfigURL();
    String dbName = extractDbFromURL(sourceDbURL);
    String username = options.getUsername();
    String password = options.getPassword();
    String namespace = options.getNamespace();

    String jdbcDriverClassName = options.getJdbcDriverClassName();
    String jdbcDriverJars = options.getJdbcDriverJars();
    long maxConnections =
        options.getMaxConnections() > 0 ? (long) (options.getMaxConnections()) : 0;
    Integer numPartitions = options.getNumPartitions();

    return getJdbcIOWrapperConfig(
        sqlDialect,
        tables,
        sourceDbURL,
        null,
        null,
        0,
        username,
        password,
        dbName,
        namespace,
        shardId,
        jdbcDriverClassName,
        jdbcDriverJars,
        maxConnections,
        numPartitions,
        waitOn);
  }

  public static JdbcIOWrapperConfig getJdbcIOWrapperConfig(
      SQLDialect sqlDialect,
      List<String> tables,
      String sourceDbURL,
      String host,
      String connectionProperties,
      int port,
      String username,
      String password,
      String dbName,
      String namespace,
      String shardId,
      String jdbcDriverClassName,
      String jdbcDriverJars,
      long maxConnections,
      Integer numPartitions,
      Wait.OnSignal<?> waitOn) {
    JdbcIOWrapperConfig.Builder builder = builderWithDefaultsFor(sqlDialect);
    SourceSchemaReference sourceSchemaReference =
        sourceSchemaReferenceFrom(sqlDialect, dbName, namespace);
    builder =
        builder
            .setSourceSchemaReference(sourceSchemaReference)
            .setDbAuth(
                LocalCredentialsProvider.builder()
                    .setUserName(
                        username) // TODO - support taking username and password from url as well
                    .setPassword(password)
                    .build())
            .setJdbcDriverClassName(jdbcDriverClassName)
            .setJdbcDriverJars(jdbcDriverJars);
    if (maxConnections != 0) {
      builder = builder.setMaxConnections(maxConnections);
    }

    switch (sqlDialect) {
      case MYSQL:
        if (sourceDbURL == null) {
          sourceDbURL = "jdbc:mysql://" + host + ":" + port + "/" + dbName;
          if (StringUtils.isNotBlank(connectionProperties)) {
            sourceDbURL = sourceDbURL + "?" + connectionProperties;
          }
        }
        for (Entry<String, String> entry :
            MySqlConfigDefaults.DEFAULT_MYSQL_URL_PROPERTIES.entrySet()) {
          sourceDbURL = addParamToJdbcUrl(sourceDbURL, entry.getKey(), entry.getValue());
        }
        break;
      case POSTGRESQL:
        if (sourceDbURL == null) {
          sourceDbURL = "jdbc:postgresql://" + host + ":" + port + "/" + dbName;
        }
        sourceDbURL = sourceDbURL + "?currentSchema=" + sourceSchemaReference.jdbc().namespace();
        if (StringUtils.isNotBlank(connectionProperties)) {
          sourceDbURL = sourceDbURL + "&" + connectionProperties;
        }
        break;
    }

    builder.setSourceDbURL(sourceDbURL);
    if (!StringUtils.isEmpty(shardId)) {
      builder.setShardID(shardId);
    }

    if (waitOn != null) {
      builder.setWaitOn(waitOn);
    }

    builder.setMaxPartitions(numPartitions);
    builder = builder.setTables(ImmutableList.copyOf(tables));
    return builder.build();
  }

  @VisibleForTesting
  protected static String addParamToJdbcUrl(String jdbcUrl, String paramName, String paramValue) {
    // URI/ URL libraries don't seem to handle jdbc URLs well
    Pattern queryPattern = Pattern.compile("\\?(.*?)$");

    Matcher matcher = queryPattern.matcher(jdbcUrl);

    String baseUrl;
    String query;

    if (matcher.find()) {
      baseUrl = jdbcUrl.substring(0, matcher.start());
      query = matcher.group(1);
    } else {
      baseUrl = jdbcUrl;
      query = null;
    }

    if (query == null) {
      // No parameters exist, add the new one
      return jdbcUrl + "?" + paramName + "=" + paramValue;
    } else {
      String[] params = query.split("&");
      boolean paramFound = false;
      StringBuilder newQuery = new StringBuilder();

      for (String param : params) {
        String[] keyValue = param.split("=");
        if (keyValue[0].equals(paramName)) {
          paramFound = true;
          if (keyValue[1].equals(paramValue)) {
            // Parameter exists with the correct value, keep it as is
            if (newQuery.length() > 0) {
              newQuery.append("&");
            }
            newQuery.append(param);
          } else {
            // Mismatch, handle based on the mismatchException flag
            throw new IllegalArgumentException(
                "Parameter mismatch for "
                    + paramName
                    + " URL = "
                    + jdbcUrl
                    + " config value = "
                    + paramValue);
          }
        } else {
          if (newQuery.length() > 0) {
            newQuery.append("&");
          }
          newQuery.append(param);
        }
      }

      if (!paramFound) {
        // Parameter doesn't exist, add it
        if (newQuery.length() > 0) {
          newQuery.append("&");
        }
        newQuery.append(paramName).append("=").append(paramValue);
      }

      return baseUrl + "?" + newQuery;
    }
  }

  private static String extractDbFromURL(String sourceDbUrl) {
    URI uri;
    try {
      // Strip off the prefix 'jdbc:' which the library cannot handle.
      uri = new URI(sourceDbUrl.substring(5));
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Unable to parse url: %s", sourceDbUrl), e);
    }
    // Remove '/' before returning.
    return uri.getPath().substring(1);
  }

  private static JdbcIOWrapperConfig.Builder builderWithDefaultsFor(SQLDialect dialect) {
    if (dialect == SQLDialect.POSTGRESQL) {
      return builderWithPostgreSQLDefaults();
    }
    return builderWithMySqlDefaults();
  }

  // TODO(vardhanvthigle): Standardize for Css.
  private static SourceSchemaReference sourceSchemaReferenceFrom(
      SQLDialect dialect, String dbName, String namespace) {
    JdbcSchemaReference.Builder builder = JdbcSchemaReference.builder();
    // Namespaces are not supported for MySQL
    if (dialect == SQLDialect.POSTGRESQL) {
      if (StringUtils.isBlank(namespace)) {
        builder.setNamespace(DEFAULT_POSTGRESQL_NAMESPACE);
      } else {
        builder.setNamespace(namespace);
      }
    }
    return SourceSchemaReference.ofJdbc(builder.setDbName(dbName).build());
  }

  private OptionsToConfigBuilder() {}
}
