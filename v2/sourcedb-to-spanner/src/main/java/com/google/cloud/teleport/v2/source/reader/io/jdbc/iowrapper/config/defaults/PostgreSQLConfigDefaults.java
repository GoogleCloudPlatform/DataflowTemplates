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
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.postgresql.PostgreSQLDialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.postgresql.PostgreSQLDialectAdapter.PostgreSQLVersion;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.provider.PostgreSQLJdbcValueMappings;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapper;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.util.FluentBackoff;
import org.joda.time.Duration;

/**
 * PostgreSQL Default Configuration for {@link
 * com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.JdbcIoWrapper JdbcIoWrapper}.
 */
public class PostgreSQLConfigDefaults {
  public static final UnifiedTypeMapper.MapperType DEFAULT_POSTGRESQL_SCHEMA_MAPPER_TYPE =
      UnifiedTypeMapper.MapperType.POSTGRESQL;
  public static final DialectAdapter DEFAULT_POSTGRESQL_DIALECT_ADAPTER =
      new PostgreSQLDialectAdapter(PostgreSQLVersion.DEFAULT);
  public static final JdbcValueMappingsProvider DEFAULT_POSTGRESQL_VALUE_MAPPING_PROVIDER =
      new PostgreSQLJdbcValueMappings();

  public static final Long DEFAULT_POSTGRESQL_MAX_CONNECTIONS = 160L;

  public static final FluentBackoff DEFAULT_POSTGRESQL_SCHEMA_DISCOVERY_BACKOFF =
      FluentBackoff.DEFAULT.withMaxCumulativeBackoff(Duration.standardMinutes(5L));

  /** Default Initialization Sequence for the JDBC connection. */
  public static final ImmutableList<String> DEFAULT_POSTGRESQL_INIT_SEQ =
      ImmutableList.of(
          // Using an offset of 0 instead of UTC, as it's possible for a customer's database to not
          // have named timezone information pre-installed.
          "SET TIME ZONE '+00:00'");

  private PostgreSQLConfigDefaults() {}
}
