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
package com.google.cloud.teleport.v2.source.cassandra;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.DbConfigContainer;
import com.google.cloud.teleport.v2.source.DbConfigContainerDefaultImpl;
import com.google.cloud.teleport.v2.source.ISourceConnector;
import com.google.cloud.teleport.v2.source.cassandra.iowrapper.CassandraIOWrapperFactory;
import com.google.cloud.teleport.v2.source.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.common.base.Preconditions;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;

/** Cassandra Source Connector. */
public class CassandraSourceConnector implements ISourceConnector {

  @Override
  public DbConfigContainer getDbConfigContainer(SourceDbToSpannerOptions options) {
    if (SourceDbToSpannerOptions.CASSANDRA_SOURCE_DIALECT.equals(options.getSourceDbDialect())) {
      Preconditions.checkArgument(
          StringUtils.isNotEmpty(options.getSourceConfigURL()),
          "Cassandra Dialect needs sourceConfigURL to be set.");
    }
    return new DbConfigContainerDefaultImpl(CassandraIOWrapperFactory.fromPipelineOptions(options));
  }

  @Override
  public JdbcIOWrapperConfig.Builder getJdbcIOWrapperConfigBuilder() {
    throw new UnsupportedOperationException("Not supported for Cassandra");
  }

  @Override
  public SourceSchemaReference getSourceSchemaReference(String dbName, String namespace) {
    throw new UnsupportedOperationException("Not supported for Cassandra");
  }

  @Override
  public void setDataSourceLoginTimeout(BasicDataSource dataSource, long timeoutMs) {
    throw new UnsupportedOperationException("Not supported for Cassandra");
  }

  @Override
  public String getJdbcUrl(
      String host,
      int port,
      String dbName,
      String namespace,
      String connectionProperties,
      Integer fetchSize,
      String sourceDbURL) {
    throw new UnsupportedOperationException("Not supported for Cassandra");
  }
}
