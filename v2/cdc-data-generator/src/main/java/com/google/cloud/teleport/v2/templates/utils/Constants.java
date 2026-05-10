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
package com.google.cloud.teleport.v2.templates.utils;

/** Constants shared across the CDC Data Generator template. */
public final class Constants {

  // Default HikariCP pool size per shard when writing to JDBC sinks.
  public static final int DEFAULT_JDBC_POOL_SIZE = 10;
  public static final int DEFAULT_STRING_LENGTH = 20;
  public static final int DEFAULT_NUMERIC_PRECISION = 10;
  public static final int DEFAULT_NUMERIC_SCALE = 2;

  // MySQL JDBC driver class name, used when constructing ConnectionHelperRequest.
  public static final String MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

  // JDBC URL prefix for MySQL connections.
  public static final String JDBC_MYSQL_URL_PREFIX = "jdbc:mysql://";

  // Character used to quote identifiers in MySQL (back-tick).
  public static final String MYSQL_IDENTIFIER_QUOTE = "`";

  // Column name used to carry the logical shard id through the pipeline. Prefixed with `_dg_`
  // so it cannot collide with a user-defined PK column.
  public static final String SHARD_ID_COLUMN_NAME = "_dg_shard_id";

  // Keys for Spanner sink configuration JSON.
  public static final String SPANNER_CONFIG_PROJECT_ID_KEY = "projectId";
  public static final String SPANNER_CONFIG_INSTANCE_ID_KEY = "instanceId";
  public static final String SPANNER_CONFIG_DATABASE_ID_KEY = "databaseId";

  public static final String SINK_TYPE_MYSQL = "MYSQL";
  public static final String SINK_TYPE_SPANNER = "SPANNER";
}
