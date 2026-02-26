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
package com.google.cloud.teleport.v2.fn;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaFileOverridesBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaStringOverridesBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SerializableFunction} that provides an {@link ISchemaMapper} given a {@link Ddl}. This
 * class encapsulates the logic for constructing the schema mapper, including handling schema
 * overrides.
 */
public class SchemaMapperProviderFn implements SerializableFunction<Ddl, ISchemaMapper> {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaMapperProviderFn.class);

  private final String sessionFilePath;
  private final String schemaOverridesFilePath;
  private final String tableOverrides;
  private final String columnOverrides;

  public SchemaMapperProviderFn(
      String sessionFilePath,
      String schemaOverridesFilePath,
      String tableOverrides,
      String columnOverrides) {
    this.sessionFilePath = sessionFilePath;
    this.schemaOverridesFilePath = schemaOverridesFilePath;
    this.tableOverrides = tableOverrides;
    this.columnOverrides = columnOverrides;
  }

  @Override
  public ISchemaMapper apply(Ddl ddl) {
    Map<String, String> schemaStringOverrides = new HashMap<>();
    if (tableOverrides != null && !tableOverrides.isEmpty()) {
      schemaStringOverrides.put("tableOverrides", tableOverrides);
    }
    if (columnOverrides != null && !columnOverrides.isEmpty()) {
      schemaStringOverrides.put("columnOverrides", columnOverrides);
    }

    return getSchemaMapper(sessionFilePath, schemaOverridesFilePath, schemaStringOverrides, ddl);
  }

  private ISchemaMapper getSchemaMapper(
      String sessionFilePath,
      String schemaOverridesFilePath,
      Map<String, String> schemaStringOverrides,
      Ddl ddl) {
    if (sessionFilePath != null && !sessionFilePath.isEmpty()) {
      LOG.info("Using SessionBasedMapper with file: {}", sessionFilePath);
      return new SessionBasedMapper(sessionFilePath, ddl);
    } else if (schemaOverridesFilePath != null && !schemaOverridesFilePath.isEmpty()) {
      LOG.info("Using SchemaFileOverridesBasedMapper with file: {}", schemaOverridesFilePath);
      return new SchemaFileOverridesBasedMapper(schemaOverridesFilePath, ddl);
    } else if (schemaStringOverrides != null && !schemaStringOverrides.isEmpty()) {
      LOG.info("Using SchemaStringOverridesBasedMapper with overrides: {}", schemaStringOverrides);
      return new SchemaStringOverridesBasedMapper(schemaStringOverrides, ddl);
    } else {
      LOG.info("Using IdentityMapper");
      return new IdentityMapper(ddl);
    }
  }
}
