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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link AvroDestination}. */
@RunWith(JUnit4.class)
public class AvroDestinationTest {

  @Test
  public void testEqualsAndHashCode() {
    AvroDestination dest1 = AvroDestination.of("table1", "schema1");
    AvroDestination dest2 = AvroDestination.of("table1", "schema1");
    AvroDestination dest3 = AvroDestination.of("table2", "schema1");
    AvroDestination dest4 = AvroDestination.of("table1", "schema2");

    assertEquals(dest1, dest2);
    assertEquals(dest1.hashCode(), dest2.hashCode());
    assertNotEquals(dest1, dest3);
    assertNotEquals(dest1, dest4);
    assertNotEquals(dest3, dest4);
  }

  @Test
  public void testOf() {
    AvroDestination destination = AvroDestination.of("tableName", "jsonSchema");
    assertEquals("tableName", destination.name);
    assertEquals("jsonSchema", destination.jsonSchema);
  }
}
