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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaFileOverridesBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaStringOverridesBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SchemaMapperProviderFnTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testGetSchemaMapper_SessionFile() throws IOException {
    Path sessionFile = temporaryFolder.newFile("session.json").toPath();
    Files.write(
        sessionFile, "{\"SpSchema\": {}, \"SrcSchema\": {}, \"SyntheticPKeys\": {}}".getBytes());
    Ddl ddl = mock(Ddl.class);

    SchemaMapperProviderFn fn =
        new SchemaMapperProviderFn(sessionFile.toString(), null, null, null);
    ISchemaMapper mapper = fn.apply(ddl);

    assertTrue(mapper instanceof SessionBasedMapper);
  }

  @Test
  public void testGetSchemaMapper_SchemaOverridesFile() throws IOException {
    Path overridesFile = temporaryFolder.newFile("overrides.json").toPath();
    Files.write(overridesFile, "{}".getBytes());
    Ddl ddl = mock(Ddl.class);

    SchemaMapperProviderFn fn =
        new SchemaMapperProviderFn(null, overridesFile.toString(), null, null);
    ISchemaMapper mapper = fn.apply(ddl);

    assertTrue(mapper instanceof SchemaFileOverridesBasedMapper);
  }

  @Test
  public void testGetSchemaMapper_StringOverrides() {
    Ddl ddl = mock(Ddl.class);
    String tableOverrides = "{{old_table, new_table}}";

    SchemaMapperProviderFn fn = new SchemaMapperProviderFn(null, null, tableOverrides, null);
    ISchemaMapper mapper = fn.apply(ddl);

    assertTrue(mapper instanceof SchemaStringOverridesBasedMapper);
  }

  @Test
  public void testGetSchemaMapper_ColumnOverrides() {
    Ddl ddl = mock(Ddl.class);
    String columnOverrides = "{\"col1\": \"newCol1\"}";

    SchemaMapperProviderFn fn = new SchemaMapperProviderFn(null, null, null, columnOverrides);
    ISchemaMapper mapper = fn.apply(ddl);

    assertTrue(mapper instanceof SchemaStringOverridesBasedMapper);
  }

  @Test
  public void testGetSchemaMapper_BothStringOverrides() {
    Ddl ddl = mock(Ddl.class);
    String tableOverrides = "{{old_table, new_table}}";
    String columnOverrides = "{\"col1\": \"newCol1\"}";

    SchemaMapperProviderFn fn =
        new SchemaMapperProviderFn(null, null, tableOverrides, columnOverrides);
    ISchemaMapper mapper = fn.apply(ddl);

    assertTrue(mapper instanceof SchemaStringOverridesBasedMapper);
  }

  @Test
  public void testGetSchemaMapper_SessionFilePriority() throws IOException {
    Path sessionFile = temporaryFolder.newFile("session_prio.json").toPath();
    Files.write(
        sessionFile, "{\"SpSchema\": {}, \"SrcSchema\": {}, \"SyntheticPKeys\": {}}".getBytes());
    Path overridesFile = temporaryFolder.newFile("overrides_ignored.json").toPath();
    Files.write(overridesFile, "{}".getBytes());
    Ddl ddl = mock(Ddl.class);

    SchemaMapperProviderFn fn =
        new SchemaMapperProviderFn(
            sessionFile.toString(), overridesFile.toString(), "{{old, new}}", null);
    ISchemaMapper mapper = fn.apply(ddl);

    assertTrue(mapper instanceof SessionBasedMapper);
  }

  @Test
  public void testGetSchemaMapper_FileOverridesPriority() throws IOException {
    Path overridesFile = temporaryFolder.newFile("overrides_prio.json").toPath();
    Files.write(overridesFile, "{}".getBytes());
    Ddl ddl = mock(Ddl.class);

    SchemaMapperProviderFn fn =
        new SchemaMapperProviderFn(null, overridesFile.toString(), "{{old, new}}", null);
    ISchemaMapper mapper = fn.apply(ddl);

    assertTrue(mapper instanceof SchemaFileOverridesBasedMapper);
  }

  @Test
  public void testGetSchemaMapper_EmptyStrings() {
    Ddl ddl = mock(Ddl.class);

    SchemaMapperProviderFn fn = new SchemaMapperProviderFn("", "", "", "");
    ISchemaMapper mapper = fn.apply(ddl);

    assertTrue(mapper instanceof IdentityMapper);
  }

  @Test
  public void testGetSchemaMapper_Identity() {
    Ddl ddl = mock(Ddl.class);

    SchemaMapperProviderFn fn = new SchemaMapperProviderFn(null, null, null, null);
    ISchemaMapper mapper = fn.apply(ddl);

    assertTrue(mapper instanceof IdentityMapper);
  }
}
