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
package com.google.cloud.teleport.v2.spanner.migrations.constants;

/** A single class to store all constants common to migrations. */
public class Constants {

  /* The source type value for MySql databases */
  public static final String MYSQL_SOURCE_TYPE = "mysql";

  /* The value for Oracle databases in the source type key */
  public static final String ORACLE_SOURCE_TYPE = "oracle";

  /* The value for Postgres databases in the source type key */
  public static final String POSTGRES_SOURCE_TYPE = "postgresql";

  /* The key for the schema name in the event json */
  public static final String EVENT_SCHEMA_KEY = "_metadata_schema";

  /* The key for the table name in the event json */
  public static final String EVENT_TABLE_NAME_KEY = "_metadata_table";

  /* The key for the uuid field of the change event */
  public static final String EVENT_UUID_KEY = "_metadata_uuid";

  /* The prefix for all metadata keys in the event json */
  public static final String EVENT_METADATA_KEY_PREFIX = "_metadata";
}
