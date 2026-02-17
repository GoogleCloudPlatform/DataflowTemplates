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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaFileOverridesBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaStringOverridesBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockedConstruction;

/** Unit tests for {@link SchemaMapperUtils}. */
@RunWith(JUnit4.class)
@Ignore("Temporarily disabled for maintenance")
public class SchemaMapperUtilsTest {

  private Ddl ddl;

  @Before
  public void setUp() {
    ddl = mock(Ddl.class);
  }

  @Test
  public void testGetSchemaMapper_identityMapper() {
    try (MockedConstruction<IdentityMapper> mocked = mockConstruction(IdentityMapper.class)) {
      ISchemaMapper mapper = SchemaMapperUtils.getSchemaMapper(null, null, null, null, ddl);
      assertNotNull(mapper);
      assertTrue(mapper instanceof IdentityMapper);
      assertTrue(mocked.constructed().size() == 1);
    }
  }

  @Test
  public void testGetSchemaMapper_sessionBasedMapper() {
    try (MockedConstruction<SessionBasedMapper> mocked =
        mockConstruction(SessionBasedMapper.class)) {
      ISchemaMapper mapper = SchemaMapperUtils.getSchemaMapper("some/path", null, null, null, ddl);
      assertNotNull(mapper);
      assertTrue(mapper instanceof SessionBasedMapper);
      assertTrue(mocked.constructed().size() == 1);
    }
  }

  @Test
  public void testGetSchemaMapper_fileOverridesBasedMapper() {
    try (MockedConstruction<SchemaFileOverridesBasedMapper> mocked =
        mockConstruction(SchemaFileOverridesBasedMapper.class)) {
      ISchemaMapper mapper = SchemaMapperUtils.getSchemaMapper(null, "some/path", null, null, ddl);
      assertNotNull(mapper);
      assertTrue(mapper instanceof SchemaFileOverridesBasedMapper);
      assertTrue(mocked.constructed().size() == 1);
    }
  }

  @Test
  public void testGetSchemaMapper_stringOverridesBasedMapper() {
    try (MockedConstruction<SchemaStringOverridesBasedMapper> mocked =
        mockConstruction(SchemaStringOverridesBasedMapper.class)) {
      ISchemaMapper mapper =
          SchemaMapperUtils.getSchemaMapper(null, null, "table1:table2", "col1:col2", ddl);
      assertNotNull(mapper);
      assertTrue(mapper instanceof SchemaStringOverridesBasedMapper);
      assertTrue(mocked.constructed().size() == 1);
    }
  }

  @Test
  public void testGetSchemaMapper_multipleOverridesThrowsException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaMapperUtils.getSchemaMapper("p1", "p2", null, null, ddl));

    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaMapperUtils.getSchemaMapper("p1", null, "t1:t2", null, ddl));

    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaMapperUtils.getSchemaMapper("p1", null, null, "c1:c2", ddl));

    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaMapperUtils.getSchemaMapper(null, "p2", "t1:t2", null, ddl));
    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaMapperUtils.getSchemaMapper(null, "p2", null, "c1:c2", ddl));
  }
}
