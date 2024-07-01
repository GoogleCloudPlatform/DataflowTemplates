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

import com.google.cloud.teleport.v2.source.reader.auth.dbauth.LocalCredentialsProvider;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OptionsToConfigBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(OptionsToConfigBuilder.class);

  public static final class MySql {

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

    public static JdbcIOWrapperConfig configWithMySqlDefaultsFromOptions(
        SourceDbToSpannerOptions options, List<String> tables, String shardId) {
      String sourceDbURL = options.getSourceDbURL();
      String dbName = extractDbFromURL(sourceDbURL);
      String username = options.getUsername();
      String password = options.getPassword();

      String jdbcDriverClassName = options.getJdbcDriverClassName();
      String jdbcDriverJars = options.getJdbcDriverJars();
      long maxConnections =
          options.getMaxConnections() > 0 ? (long) (options.getMaxConnections()) : 0;
      Integer numPartitions = options.getNumPartitions();

      return getJdbcIOWrapperConfig(
          tables,
          sourceDbURL,
          null,
          0,
          username,
          password,
          dbName,
          shardId,
          jdbcDriverClassName,
          jdbcDriverJars,
          maxConnections,
          numPartitions);
    }
  }

  public static JdbcIOWrapperConfig getJdbcIOWrapperConfig(
      List<String> tables,
      String sourceDbURL,
      String host,
      int port,
      String username,
      String password,
      String dbName,
      String shardId,
      String jdbcDriverClassName,
      String jdbcDriverJars,
      long maxConnections,
      Integer numPartitions) {
    JdbcIOWrapperConfig.Builder builder = builderWithMySqlDefaults();
    builder =
        builder
            .setSourceSchemaReference(SourceSchemaReference.builder().setDbName(dbName).build())
            .setDbAuth(
                LocalCredentialsProvider.builder()
                    .setUserName(username)
                    .setPassword(password)
                    .build())
            .setJdbcDriverClassName(jdbcDriverClassName)
            .setJdbcDriverJars(jdbcDriverJars);

    if (sourceDbURL != null) {
      builder.setSourceDbURL(sourceDbURL);
    } else {
      // TODO - add mysql specific method in mysql class.
      builder.setSourceDbURL("jdbc:mysql://" + host + ":" + port + "/" + dbName);
    }
    if (!StringUtils.isEmpty(shardId)) {
      builder.setShardID(shardId);
    }

    builder.setMaxPartitions(numPartitions);
    builder = builder.setTables(ImmutableList.copyOf(tables));
    return builder.build();
  }

  private OptionsToConfigBuilder() {}
}
