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
import org.apache.beam.sdk.util.FluentBackoff;

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

  public static final boolean DEFAULT_MYSQL_AUTO_RECONNECT = true;

  public static final long DEFAULT_MYSQL_RECONNECT_ATTEMPTS = 10L;
  public static final FluentBackoff DEFAULT_MYSQL_SCHEMA_DISCOVERY_BACKOFF = FluentBackoff.DEFAULT;

  private MySqlConfigDefaults() {}
}
