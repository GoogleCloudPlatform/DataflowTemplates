/*
 * Copyright (C) 2025 Google LLC
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

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaFileOverridesBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaStringOverridesBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;

public class SchemaMapperUtils {

  public static ISchemaMapper getSchemaMapper(
      String sessionFilePath,
      String schemaOverridesFilePath,
      String tableOverrides,
      String columnOverrides,
      Ddl ddl) {
    // Check if config types are specified
    boolean hasSessionFile = sessionFilePath != null && !sessionFilePath.equals("");
    boolean hasSchemaOverridesFile =
        schemaOverridesFilePath != null && !schemaOverridesFilePath.equals("");
    boolean hasStringOverrides =
        (tableOverrides != null && !tableOverrides.equals(""))
            || (columnOverrides != null && !columnOverrides.equals(""));

    int overrideTypesCount = 0;
    if (hasSessionFile) {
      overrideTypesCount++;
    }
    if (hasSchemaOverridesFile) {
      overrideTypesCount++;
    }
    if (hasStringOverrides) {
      overrideTypesCount++;
    }

    if (overrideTypesCount > 1) {
      throw new IllegalArgumentException(
          "Only one type of schema override can be specified. Please use only one of: sessionFilePath, "
              + "schemaOverridesFilePath, or tableOverrides/columnOverrides.");
    }

    ISchemaMapper schemaMapper = new IdentityMapper(ddl);
    if (hasSessionFile) {
      schemaMapper = new SessionBasedMapper(sessionFilePath, ddl);
    } else if (hasSchemaOverridesFile) {
      schemaMapper = new SchemaFileOverridesBasedMapper(schemaOverridesFilePath, ddl);
    } else if (hasStringOverrides) {
      schemaMapper = new SchemaStringOverridesBasedMapper(tableOverrides, columnOverrides, ddl);
    }
    return schemaMapper;
  }
}
