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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.defaults;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter.MySqlVersion;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.provider.MysqlJdbcValueMappings;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapper.MapperType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Calendar;
import org.apache.beam.sdk.util.FluentBackoff;
import org.joda.time.Duration;

// TODO: Fine-tune the defaults based on benchmarking.

/**
 * MySql Default Configuration for {@link
 * com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.JdbcIoWrapper JdbcIoWrapper}.
 */
public class MySqlConfigDefaults {

  public static final MapperType DEFAULT_MYSQL_SCHEMA_MAPPER_TYPE = MapperType.MYSQL;
  public static final DialectAdapter DEFAULT_MYSQL_DIALECT_ADAPTER =
      new MysqlDialectAdapter(MySqlVersion.DEFAULT);
  public static final JdbcValueMappingsProvider DEFAULT_MYSQL_VALUE_MAPPING_PROVIDER =
      new MysqlJdbcValueMappings();

  public static final String DEFAULT_MYSQL_CONNECTION_PROPERTIES =
      "maxTotal=160;maxpoolsize=160;maxIdle=160;minIdle=160"
          + ";wait_timeout=57600"
          + ";interactive_timeout=57600"
          + ";idletimeout=3600"
          + ";maxwaittime=600_000"
          + ";maxWaitMillis=600_000"
          + ";maxConnLifetimeMillis=600_000"
          + ";testOnCreate=true;testOnBorrow=true;testOnReturn=true;testWhileIdle=true";

  public static final Long DEFAULT_MYSQL_MAX_CONNECTIONS = 160L;

  /**
   * URL settings for MySQL. allowMultiQueries is required for multi line collation discovery query.
   * autoReconnect and maxReconnects help to re-establish connection for transient failures.
   */
  public static final ImmutableMap<String, String> DEFAULT_MYSQL_URL_PROPERTIES =
      ImmutableMap.of(
          "allowMultiQueries", "true",
          "autoReconnect", "true",
          "maxReconnects", "10");

  public static final FluentBackoff DEFAULT_MYSQL_SCHEMA_DISCOVERY_BACKOFF =
      FluentBackoff.DEFAULT.withMaxCumulativeBackoff(Duration.standardMinutes(5L));

  /** Init Seq for enable ANSI Quotes. * */
  @VisibleForTesting
  public static final String ENABLE_ANSI_QUOTES_INIT_SEQ =
      "SET SESSION sql_mode = \n"
          + "  CASE \n"
          + "    WHEN @@sql_mode LIKE '%ANSI_QUOTES%' THEN @@sql_mode \n"
          + "    ELSE CONCAT(@@sql_mode, ',ANSI_QUOTES') \n"
          + "  END;";

  /**
   * Default Initialization Sequence for the JDBC connection.
   *
   * <p>
   *
   * <ol>
   *   <li>Session Timezone: session time zone is set to UTC to always retrieve timestamp in UTC.
   *       The most idomatic way to achieve this via jdbc would be to pass a {@link Calendar} object
   *       initialized to UTC to {@link java.sql.ResultSet#getTimestamp(String, Calendar)} api
   *       (which is what we do in {@link MysqlJdbcValueMappings}), but due to bugs like <a
   *       href="https://bugs.mysql.com/bug.php?id=95644">Bug#95644</a>, <a
   *       href="https://bugs.mysql.com/bug.php?id=96276">Bug#96276</a>, <a
   *       href="https://bugs.mysql.com/bug.php?id=93444">Bug#93444</a>, etc. in the older drivers,
   *       we are also setting the session timezone to UTC.
   * </ol>
   */
  // TODO: Add innodb_parallel_read_threads for better performance tuning.
  public static final ImmutableList<String> DEFAULT_MYSQL_INIT_SEQ =
      ImmutableList.of(
          // Using an offset of 0 instead of UTC, as it's possible for a customer's database to not
          // have named timezone information pre-installed.
          "SET TIME_ZONE = '+00:00'",
          "SET SESSION NET_WRITE_TIMEOUT=1200",
          "SET SESSION NET_READ_TIMEOUT=1200",
          ENABLE_ANSI_QUOTES_INIT_SEQ);

  private MySqlConfigDefaults() {}
}
