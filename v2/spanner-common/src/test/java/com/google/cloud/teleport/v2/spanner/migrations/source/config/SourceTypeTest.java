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
package com.google.cloud.teleport.v2.spanner.migrations.source.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import com.google.cloud.teleport.v2.spanner.source.SourceConstants;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SourceType} and source type constants. */
@RunWith(JUnit4.class)
public class SourceTypeTest {

  @Test
  public void testParseSourceTypeSqlServer() {
    assertEquals(SourceType.SQLSERVER, SourceType.parseSourceType("sqlserver"));
    assertEquals(SourceType.SQLSERVER, SourceType.parseSourceType("SQLSERVER"));
    assertEquals(SourceType.SQLSERVER, SourceType.parseSourceType("SqlServer"));
  }

  @Test
  public void testParseSourceTypeOtherDatabases() {
    assertEquals(SourceType.MYSQL, SourceType.parseSourceType("mysql"));
    assertEquals(SourceType.MYSQL, SourceType.parseSourceType("MYSQL"));
    assertEquals(SourceType.PG, SourceType.parseSourceType("postgresql"));
    assertEquals(SourceType.PG, SourceType.parseSourceType("POSTGRESQL"));
    assertEquals(SourceType.ORACLE, SourceType.parseSourceType("oracle"));
    assertEquals(SourceType.ORACLE, SourceType.parseSourceType("ORACLE"));
    assertEquals(SourceType.CASSANDRA, SourceType.parseSourceType("cassandra"));
    assertEquals(SourceType.ASTRA_DB, SourceType.parseSourceType("astra_db"));
  }

  @Test
  public void testParseSourceTypeUnsupportedThrowsException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> SourceType.parseSourceType("unsupported"));
    assertTrue(exception.getMessage().contains("Unsupported source type: unsupported"));
  }

  @Test
  public void testSourceTypeValuesContainSqlServer() {
    assertTrue(Arrays.asList(SourceType.values()).contains(SourceType.SQLSERVER));
    assertEquals(SourceType.SQLSERVER, SourceType.valueOf("SQLSERVER"));
  }

  @Test
  public void testSqlServerConstants() {
    assertEquals("sqlserver", Constants.SQLSERVER_SOURCE_TYPE);
    assertEquals("sqlserver", SourceConstants.SQLSERVER_SOURCE_TYPE);
  }

  @Test
  public void testPrivateConstructors() throws Exception {
    java.lang.reflect.Constructor<Constants> c1 = Constants.class.getDeclaredConstructor();
    c1.setAccessible(true);
    c1.newInstance();

    java.lang.reflect.Constructor<SourceConstants> c2 =
        SourceConstants.class.getDeclaredConstructor();
    c2.setAccessible(true);
    c2.newInstance();
  }
}
