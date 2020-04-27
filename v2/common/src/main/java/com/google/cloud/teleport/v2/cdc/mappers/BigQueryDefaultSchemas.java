/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


package com.google.cloud.teleport.v2.cdc.mappers;

import com.google.cloud.bigquery.LegacySQLTypeName;
import java.util.HashMap;
import java.util.Map;

/**
 * Class {@link BigQueryDefaultSchemas}.
 */
public final class BigQueryDefaultSchemas {

  public static final Map<String, LegacySQLTypeName> WEB_SDK_SCHEMA = new HashMap<String, LegacySQLTypeName>() {{
    put("$os", LegacySQLTypeName.STRING);
    put("$browser", LegacySQLTypeName.STRING);
    put("$referrer", LegacySQLTypeName.STRING);
    put("$referring_domain", LegacySQLTypeName.STRING);
    put("$current_url", LegacySQLTypeName.STRING);
    put("$browser_version", LegacySQLTypeName.STRING);
    put("$screen_height", LegacySQLTypeName.STRING);
    put("$screen_width", LegacySQLTypeName.STRING);
    put("mp_lib", LegacySQLTypeName.STRING);
    put("$lib_version", LegacySQLTypeName.STRING);
    put("hostname", LegacySQLTypeName.STRING);
    put("$initial_referrer", LegacySQLTypeName.STRING);
    put("$initial_referring_domain", LegacySQLTypeName.STRING);
    put("branch", LegacySQLTypeName.STRING);
  }};

  public static final Map<String, LegacySQLTypeName> DEMO_SCHEMA =
      new HashMap<String, LegacySQLTypeName>() {
        {
          put("_metadata_deleted", LegacySQLTypeName.BOOLEAN);
          put("_metadata_timestamp", LegacySQLTypeName.TIMESTAMP);
        }
      };
}
