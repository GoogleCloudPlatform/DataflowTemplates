/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.v2.cdc.mappers;

import com.google.cloud.bigquery.StandardSQLTypeName;
import java.util.HashMap;
import java.util.Map;

/** Class {@link BigQueryDefaultSchemas}. */
public final class BigQueryDefaultSchemas {

  public static final Map<String, StandardSQLTypeName> WEB_SDK_SCHEMA =
      new HashMap<String, StandardSQLTypeName>() {
        {
          put("$os", StandardSQLTypeName.STRING);
          put("$browser", StandardSQLTypeName.STRING);
          put("$referrer", StandardSQLTypeName.STRING);
          put("$referring_domain", StandardSQLTypeName.STRING);
          put("$current_url", StandardSQLTypeName.STRING);
          put("$browser_version", StandardSQLTypeName.STRING);
          put("$screen_height", StandardSQLTypeName.STRING);
          put("$screen_width", StandardSQLTypeName.STRING);
          put("mp_lib", StandardSQLTypeName.STRING);
          put("$lib_version", StandardSQLTypeName.STRING);
          put("hostname", StandardSQLTypeName.STRING);
          put("$initial_referrer", StandardSQLTypeName.STRING);
          put("$initial_referring_domain", StandardSQLTypeName.STRING);
          put("branch", StandardSQLTypeName.STRING);
        }
      };

  public static final Map<String, StandardSQLTypeName> DATASTREAM_METADATA_SCHEMA =
      new HashMap<String, StandardSQLTypeName>() {
        {
          put("_metadata_change_type", StandardSQLTypeName.STRING);
          put("_metadata_deleted", StandardSQLTypeName.BOOL);
          put("_metadata_timestamp", StandardSQLTypeName.TIMESTAMP);
          put("_metadata_read_timestamp", StandardSQLTypeName.TIMESTAMP);

          // Oracle specific metadata
          put("_metadata_row_id", StandardSQLTypeName.STRING);
          put("_metadata_rs_id", StandardSQLTypeName.STRING);

          // MySQL Specific Metadata
          put("_metadata_log_file", StandardSQLTypeName.STRING);
          put("_metadata_log_position", StandardSQLTypeName.INT64);
        }
      };
}
