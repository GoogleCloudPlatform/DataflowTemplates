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
package com.google.cloud.teleport.v2.reader.io.schema.typemapping;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.reader.io.schema.typemapping.provider.unified.Unsupported;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link UnifiedTypeMapper}. */
@RunWith(MockitoJUnitRunner.class)
public class UnifiedTypeMapperTest {
  @Test
  public void testUnifiedTypeMapper() {
    ImmutableMap<String, UnifiedTypeMapping> mockMapping =
        ImmutableMap.of("BIGINT", (mods, arrayBounds) -> SchemaBuilder.builder().longType());

    UnifiedTypeMapper mapper = new UnifiedTypeMapper(mockMapping);

    assertThat(mapper.getSchema(new SourceColumnType("bigint", null, null)))
        .isEqualTo(SchemaBuilder.builder().unionOf().nullType().and().longType().endUnion());
    Schema unknownType =
        mapper.getSchema(new SourceColumnType("NewTypeUnknownToTheWorld", null, null));
    assertThat(new Unsupported().isUnsupported(unknownType)).isTrue();
  }

  @Test
  public void testPreconditions() {
    Assert.assertThrows(java.lang.NullPointerException.class, () -> new UnifiedTypeMapper(null));
  }
}
