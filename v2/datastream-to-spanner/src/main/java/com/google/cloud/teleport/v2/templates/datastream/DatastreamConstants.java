/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.templates.datastream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

/** A single class to store all constants related to Datastream. */
public class DatastreamConstants {

  /* The key for the event change type in the event json */
  public static final String EVENT_CHANGE_TYPE_KEY = "_metadata_change_type";

  public static final String INSERT_EVENT = "INSERT";

  public static final String UPDATE_EVENT = "UPDATE";

  public static final String MYSQL_UPDATE_EVENT = "UPDATE-INSERT";

  public static final String DELETE_EVENT = "DELETE";

  public static final String EMPTY_EVENT = "";

  /* The key for the source database type in the event json */
  public static final String EVENT_SOURCE_TYPE_KEY = "_metadata_source_type";

  /* The source type value for MySql databases */
  public static final String MYSQL_SOURCE_TYPE = "mysql";

  /* The value for Oracle databases in the source type key */
  public static final String ORACLE_SOURCE_TYPE = "oracle";

  /* The value for Postgres databases in the source type key */
  public static final String POSTGRES_SOURCE_TYPE = "postgresql";

  /* The key for the table name in the event json */
  public static final String EVENT_TABLE_NAME_KEY = "_metadata_table";

  /* The key for the uuid field of the change event */
  public static final String EVENT_UUID_KEY = "_metadata_uuid";

  /* The key for the read method in the event json */
  public static final String EVENT_READ_METHOD_KEY = "_metadata_read_method";

  /* The prefix for all metadata keys in the event json */
  public static final String EVENT_METADATA_KEY_PREFIX = "_metadata";

  /* List of Event keys, Shadow table information related to sort order in Oracle database. */
  public static final String ORACLE_TIMESTAMP_KEY = "_metadata_timestamp";

  public static final Pair<String, String> ORACLE_TIMESTAMP_SHADOW_INFO =
      Pair.of("timestamp", "INT64");

  public static final String ORACLE_SCN_KEY = "_metadata_scn";

  public static final Pair<String, String> ORACLE_SCN_SHADOW_INFO = Pair.of("scn", "INT64");

  /* Mapping from Event keys to shadow table information for Oracle database */
  public static final Map<String, Pair<String, String>> ORACLE_SORT_ORDER;

  /* List of Event keys, Shadow table information related to sort order in MySQL database. */
  public static final String MYSQL_TIMESTAMP_KEY = "_metadata_timestamp";

  public static final Pair<String, String> MYSQL_TIMESTAMP_SHADOW_INFO =
      Pair.of("timestamp", "INT64");

  public static final String MYSQL_LOGFILE_KEY = "_metadata_log_file";

  public static final Pair<String, String> MYSQL_LOGFILE_SHADOW_INFO =
      Pair.of("log_file", "STRING(MAX)");

  public static final String MYSQL_LOGPOSITION_KEY = "_metadata_log_position";

  public static final Pair<String, String> MYSQL_LOGPOSITION_SHADOW_INFO =
      Pair.of("log_position", "INT64");

  /* Mapping from Event keys to shadow table information for MySql database */
  public static final Map<String, Pair<String, String>> MYSQL_SORT_ORDER;

  /* List of Event keys, Shadow table information related to sort order in Postgres database. */
  public static final String POSTGRES_TIMESTAMP_KEY = "_metadata_timestamp";

  public static final Pair<String, String> POSTGRES_TIMESTAMP_SHADOW_INFO =
      Pair.of("timestamp", "INT64");

  public static final String POSTGRES_LSN_KEY = "_metadata_lsn";

  public static final Pair<String, String> POSTGRES_LSN_SHADOW_INFO = Pair.of("lsn", "STRING(MAX)");

  /* Mapping from Event keys to shadow table information for Postgres database */
  public static final Map<String, Pair<String, String>> POSTGRES_SORT_ORDER;

  /* List of supported */
  public static final List<String> SUPPORTED_DATASTREAM_SOURCES;

  static {
    ORACLE_SORT_ORDER =
        ImmutableMap.of(
            ORACLE_TIMESTAMP_KEY, ORACLE_TIMESTAMP_SHADOW_INFO,
            ORACLE_SCN_KEY, ORACLE_SCN_SHADOW_INFO);

    MYSQL_SORT_ORDER =
        ImmutableMap.of(
            MYSQL_TIMESTAMP_KEY, MYSQL_TIMESTAMP_SHADOW_INFO,
            MYSQL_LOGFILE_KEY, MYSQL_LOGFILE_SHADOW_INFO,
            MYSQL_LOGPOSITION_KEY, MYSQL_LOGPOSITION_SHADOW_INFO);

    POSTGRES_SORT_ORDER =
        ImmutableMap.of(
            POSTGRES_TIMESTAMP_KEY, POSTGRES_TIMESTAMP_SHADOW_INFO,
            POSTGRES_LSN_KEY, POSTGRES_LSN_SHADOW_INFO);

    SUPPORTED_DATASTREAM_SOURCES =
        ImmutableList.of(ORACLE_SOURCE_TYPE, MYSQL_SOURCE_TYPE, POSTGRES_SOURCE_TYPE);
  }
}
