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
package com.google.cloud.teleport.v2.source.jdbc;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.DbConfigContainer;
import com.google.cloud.teleport.v2.source.ISourceConnector;
import com.google.cloud.teleport.v2.source.SQLDialect;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JDBC Source Connector. */
public abstract class JdbcSourceConnector implements ISourceConnector {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceConnector.class);

  protected void setConnectionProperty(BasicDataSource dataSource, String property, String value) {
    String url = dataSource.getUrl();
    if (url == null || !url.contains(property)) {
      dataSource.addConnectionProperty(property, value);
      LOG.info("Set {} = {}  for schema discovery of {}", property, value, dataSource);
    } else {
      LOG.warn(
          "Property {} already set in URL {}. Not overriding with {} for schema discovery.",
          property,
          url,
          value);
    }
  }

  @Override
  public DbConfigContainer getDbConfigContainer(SourceDbToSpannerOptions options) {
    Preconditions.checkArgument(
        StringUtils.isNotEmpty(options.getSourceConfigURL()),
        "JDBC based source needs sourceConfigURL to be set.");

    if (options.getSourceConfigURL().startsWith("gs://")) {
      List<Shard> shards =
          new ShardFileReader(new SecretManagerAccessorImpl())
              .readForwardMigrationShardingConfig(options.getSourceConfigURL());
      SQLDialect sqlDialect = SQLDialect.valueOf(options.getSourceDbDialect());
      return new ShardedJdbcDbConfigContainer(this, shards, sqlDialect, options);
    } else {
      return new SingleInstanceJdbcDbConfigContainer(this, options);
    }
  }
}
