/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link Decimal}. */
@RunWith(MockitoJUnitRunner.class)
public class DecimalTest {

  @Test
  public void testGetSchemaForScaleAndPrecision() {
    final Long[] mods = {3L, 2L};
    assertThat(new Decimal().getSchema(mods, null).toString())
        .isEqualTo("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":3,\"scale\":2}");
  }

  @Test
  public void testGetSchemaPreconditions() {

    final Long[] modsNull = null;
    final Long[] modsEmpty = {};
    final Long[] modsNoPrecision = {1L};
    final Long[] mods = {1L, 1L};
    final Long[] arrayBounds = {1L};

    Assert.assertThrows(
        java.lang.IllegalArgumentException.class, () -> new Decimal().getSchema(modsNull, null));
    Assert.assertThrows(
        java.lang.IllegalArgumentException.class, () -> new Decimal().getSchema(modsEmpty, null));
    Assert.assertThrows(
        java.lang.IllegalArgumentException.class,
        () -> new Decimal().getSchema(modsNoPrecision, null));

    Assert.assertThrows(
        java.lang.IllegalArgumentException.class, () -> new Decimal().getSchema(mods, arrayBounds));
  }
}
