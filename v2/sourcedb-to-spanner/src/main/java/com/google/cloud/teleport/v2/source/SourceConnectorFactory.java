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
import com.google.cloud.teleport.v2.source.cassandra.CassandraSourceConnector;
import com.google.cloud.teleport.v2.source.mysql.MySqlSourceConnector;
import com.google.cloud.teleport.v2.source.postgres.PostgreSqlSourceConnector;

/** Factory for creating {@link ISourceConnector} instances. */
public class SourceConnectorFactory {

  public static ISourceConnector create(SourceDbToSpannerOptions options) {
    switch (options.getSourceDbDialect()) {
      case SourceDbToSpannerOptions.CASSANDRA_SOURCE_DIALECT:
      case SourceDbToSpannerOptions.ASTRA_DB_SOURCE_DIALECT:
        return new CassandraSourceConnector();
      case SourceDbToSpannerOptions.MYSQL_SOURCE_DIALECT:
        return new MySqlSourceConnector();
      case SourceDbToSpannerOptions.PG_SOURCE_DIALECT:
        return new PostgreSqlSourceConnector();
      default:
        throw new IllegalArgumentException("Unsupported dialect: " + options.getSourceDbDialect());
    }
  }
}
