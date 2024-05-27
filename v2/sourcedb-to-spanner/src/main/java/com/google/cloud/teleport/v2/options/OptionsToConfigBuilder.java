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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OptionsToConfigBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(OptionsToConfigBuilder.class);

  public static final class MySql {

    private static String extractDbFromURL(String url) {
      URI uri;
      try {
        uri = new URI(url);
      } catch (URISyntaxException e) {
        throw new RuntimeException(String.format("Unable to parse url: %s", url), e);
      }
      // Remove '/' before returning.
      return uri.getPath().substring(1);
    }

    public static JdbcIOWrapperConfig configWithMySqlDefaultsFromOptions(
        SourceDbToSpannerOptions options, List<String> tables) {
      JdbcIOWrapperConfig.Builder builder = builderWithMySqlDefaults();
      builder =
          builder
              .setSourceDbURL(options.getSourceDbURL())
              .setSourceSchemaReference(
                  SourceSchemaReference.builder()
                      .setDbName(
                          // Strip off the prefix 'jdbc:' which the library cannot handle.
                          extractDbFromURL(options.getSourceDbURL().substring(5)))
                      .build())
              .setDbAuth(
                  LocalCredentialsProvider.builder()
                      .setUserName(options.getUsername())
                      .setPassword(options.getPassword())
                      .build())
              .setJdbcDriverClassName(options.getJdbcDriverClassName())
              .setJdbcDriverJars(options.getJdbcDriverJars())
              .setShardID("Unsupported"); /*TODO: Support Sharded Migration */
      if (options.getMaxConnections() != 0) {
        builder.setMaxConnections((long) options.getMaxConnections());
      }
      builder.setMaxPartitions(options.getNumPartitions());
      builder = builder.setTables(ImmutableList.copyOf(tables));
      return builder.build();
    }
  }

  private OptionsToConfigBuilder() {}
}
