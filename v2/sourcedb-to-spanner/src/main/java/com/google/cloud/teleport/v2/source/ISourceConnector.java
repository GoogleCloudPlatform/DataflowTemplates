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
package com.google.cloud.teleport.v2.source;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import org.apache.commons.dbcp2.BasicDataSource;

/**
 * Interface for Source Connectors. Holds references to all source specific code and separates
 * source specific logic.
 */
public interface ISourceConnector {

  /**
   * Gets the {@link DbConfigContainer} for the source.
   *
   * @param options Pipeline options.
   * @return DbConfigContainer.
   */
  DbConfigContainer getDbConfigContainer(SourceDbToSpannerOptions options);

  /** Gets the {@link JdbcIOWrapperConfig.Builder} with defaults for the source. */
  JdbcIOWrapperConfig.Builder getJdbcIOWrapperConfigBuilder();

  /** Gets the {@link SourceSchemaReference} for the source. */
  SourceSchemaReference getSourceSchemaReference(String dbName, String namespace);

  /** Sets the login timeout for the datasource. */
  void setDataSourceLoginTimeout(BasicDataSource dataSource, long timeoutMs);

  /** Gets the JDBC URL for the source. */
  String getJdbcUrl(
      String host,
      int port,
      String dbName,
      String namespace,
      String connectionProperties,
      Integer fetchSize,
      String sourceDbURL);
}
