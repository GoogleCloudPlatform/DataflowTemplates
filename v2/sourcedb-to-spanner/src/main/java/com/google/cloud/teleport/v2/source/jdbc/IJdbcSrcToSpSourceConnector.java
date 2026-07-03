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

import com.google.cloud.teleport.v2.source.ISrcToSpSourceConnector;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;

/**
 * Interface for JDBC source connectors. Extends {@link ISrcToSpSourceConnector} with JDBC specific
 * methods.
 */
public interface IJdbcSrcToSpSourceConnector extends ISrcToSpSourceConnector {

  /** Gets the dialect adapter for the JDBC source. */
  DialectAdapter getDialectAdapter();

  /** Gets the JDBC value mappings provider. */
  JdbcValueMappingsProvider getJdbcValueMappingsProvider();

  /** Gets the JDBC IO wrapper configuration builder with source-specific defaults. */
  JdbcIOWrapperConfig.Builder getJdbcIOWrapperConfigBuilder();

  // TODO(vardhanvthigle): Standardize for Css.
  /** Gets the SourceSchemaReference. */
  SourceSchemaReference getSourceSchemaReference(String dbName, String namespace);

  /** Gets the JDBC URL with source-specific properties added. */
  String getJdbcUrl(
      String jdbcUrl,
      String host,
      int port,
      String dbName,
      String connectionProperties,
      String namespace,
      Integer fetchSize);
}
