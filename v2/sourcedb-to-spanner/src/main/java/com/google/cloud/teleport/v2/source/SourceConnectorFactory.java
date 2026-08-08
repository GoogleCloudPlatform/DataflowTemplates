/*
 * Copyright (C) 2026 Google LLC
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
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.source.cassandra.CassandraSrcToSpSourceConnector;
import com.google.cloud.teleport.v2.source.jdbc.AbstractJdbcSrcToSpSourceConnector;
import com.google.cloud.teleport.v2.source.mysql.MySqlSrcToSpSourceConnector;
import com.google.cloud.teleport.v2.source.neo4j.Neo4jSrcToSpSourceConnector;
import com.google.cloud.teleport.v2.source.postgres.PostgresSrcToSpSourceConnector;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;

/** Factory to create {@link ISrcToSpSourceConnector} instances based on pipeline options. */
public class SourceConnectorFactory {

  /**
   * Gets the appropriate {@link ISrcToSpSourceConnector} for the configured source database
   * dialect.
   *
   * @param options Pipeline options.
   * @return The source connector.
   */
  public static ISrcToSpSourceConnector getSourceConnectorByDialect(
      SourceDbToSpannerOptions options) {
    String dialect = options.getSourceDbDialect();
    if (SourceDbToSpannerOptions.CASSANDRA_SOURCE_DIALECT.equals(dialect)
        || SourceDbToSpannerOptions.ASTRA_DB_SOURCE_DIALECT.equals(dialect)) {
      return new CassandraSrcToSpSourceConnector();
    } else if (SourceDbToSpannerOptions.MYSQL_SOURCE_DIALECT.equals(dialect)) {
      return new MySqlSrcToSpSourceConnector();
    } else if (SourceDbToSpannerOptions.PG_SOURCE_DIALECT.equals(dialect)) {
      return new PostgresSrcToSpSourceConnector();
    } else if (SourceDbToSpannerOptions.NEO4J_SOURCE_DIALECT.equals(dialect)) {
      return new Neo4jSrcToSpSourceConnector();
    }
    /* Implementation detail, not having a default leads to failure in compile time checks enforced here */
    throw new IllegalArgumentException("Unsupported source database dialect: " + dialect);
  }

  /**
   * Gets the appropriate {@link ISrcToSpSourceConnector} for the configured source database type.
   * //TODO - check for ways to deduplicate this with the options method.
   *
   * @param sourceType
   * @return
   */
  public static ISrcToSpSourceConnector getSourceConnectorBySourceType(String sourceType) {
    if (sourceType == null) {
      throw new IllegalArgumentException("Source type not provided");
    }
    switch (sourceType) {
      case Constants.MYSQL_SOURCE_TYPE:
        return new MySqlSrcToSpSourceConnector();
      case Constants.POSTGRES_SOURCE_TYPE:
        return new PostgresSrcToSpSourceConnector();
      case Constants.CASSANDRA_SOURCE_TYPE:
        return new CassandraSrcToSpSourceConnector();
      case Constants.NEO4J_SOURCE_TYPE:
        return new Neo4jSrcToSpSourceConnector();
      default:
        throw new IllegalArgumentException("Unsupported source type: " + sourceType);
    }
  }

  /**
   * Gets the appropriate {@link AbstractJdbcSrcToSpSourceConnector} for the given {@link
   * SQLDialect}.
   *
   * @param dialect The SQL dialect.
   * @return The JDBC source connector.
   */
  public static AbstractJdbcSrcToSpSourceConnector getSourceJdbcConnectorByDialect(
      SQLDialect dialect) {
    if (dialect == SQLDialect.MYSQL) {
      return new MySqlSrcToSpSourceConnector();
    } else if (dialect == SQLDialect.POSTGRESQL) {
      return new PostgresSrcToSpSourceConnector();
    }
    throw new IllegalArgumentException("Unsupported SQL dialect: " + dialect);
  }
}
