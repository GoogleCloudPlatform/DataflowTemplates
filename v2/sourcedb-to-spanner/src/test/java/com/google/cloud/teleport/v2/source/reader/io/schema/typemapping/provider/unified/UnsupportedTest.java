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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link Unsupported}. */
@RunWith(MockitoJUnitRunner.class)
public class UnsupportedTest {

  @Test
  public void getSchema() {
    assertThat(new Unsupported().getSchema(null, null).getType())
        .isEqualTo(SchemaBuilder.builder().nullType().getType());
    assertThat(new Unsupported().getSchema(null, null).getLogicalType().getName())
        .isEqualTo(Unsupported.UNSUPPORTED_LOGICAL_TYPE_NAME);
  }

  @Test
  public void isUnsupported() {
    Schema unsupportedSchema = new Unsupported().getSchema(null, null);
    Schema integerSchema = SchemaBuilder.builder().intType();
    assertThat(new Unsupported().isUnsupported(unsupportedSchema)).isTrue();
    assertThat(new Unsupported().isUnsupported(integerSchema)).isFalse();
  }
}
