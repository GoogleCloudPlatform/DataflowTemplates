/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.spanner.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.Dialect;
import kotlin.Pair;
import org.junit.jupiter.api.Test;

class NameUtilsTest {

  @Test
  void quoteIdentifier() {
    String quoted = NameUtils.quoteIdentifier("Schema.Table", Dialect.POSTGRESQL);
    assertEquals("\"Schema\".\"Table\"", quoted);

    quoted = NameUtils.quoteIdentifier("Table", Dialect.POSTGRESQL);
    assertEquals("\"Table\"", quoted);

    quoted = NameUtils.quoteIdentifier("Default.Table", Dialect.GOOGLE_STANDARD_SQL);
    assertEquals("`Default`.`Table`", quoted);

    quoted = NameUtils.quoteIdentifier("Table", Dialect.GOOGLE_STANDARD_SQL);
    assertEquals("`Table`", quoted);
  }

  @Test
  void splitName() {
    Pair<String, String> paths = NameUtils.splitName("Schema.Table", Dialect.POSTGRESQL);
    assertEquals(new Pair<>("Schema", "Table"), paths);

    paths = NameUtils.splitName("Table", Dialect.POSTGRESQL);
    assertEquals(new Pair<>("public", "Table"), paths);

    assertThrows(
        IllegalArgumentException.class,
        () -> NameUtils.splitName("Illegal.Schema.Table", Dialect.POSTGRESQL));

    paths = NameUtils.splitName("Schema.Table", Dialect.GOOGLE_STANDARD_SQL);
    assertEquals(new Pair<>("Schema", "Table"), paths);

    paths = NameUtils.splitName("Table", Dialect.GOOGLE_STANDARD_SQL);
    assertEquals(new Pair<>("", "Table"), paths);

    assertThrows(
        IllegalArgumentException.class,
        () -> NameUtils.splitName("Illegal.Schema.Table", Dialect.GOOGLE_STANDARD_SQL));
  }

  @Test
  void qualifyName() {
    assertEquals("Schema.Table", NameUtils.getQualifiedName("Schema", "Table"));

    assertEquals("Table", NameUtils.getQualifiedName("", "Table"));

    assertEquals("Table", NameUtils.getQualifiedName("public", "Table"));

    assertEquals("Table", NameUtils.getQualifiedName(null, "Table"));
  }
}
