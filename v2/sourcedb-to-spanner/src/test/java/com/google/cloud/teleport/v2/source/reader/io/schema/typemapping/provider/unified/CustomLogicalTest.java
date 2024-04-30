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

import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomLogical.Json;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomLogical.Number;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomLogical.TimeIntervalMicros;
import org.apache.avro.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link CustomLogical}. */
@RunWith(MockitoJUnitRunner.class)
public class CustomLogicalTest {
  @Test
  public void testJson() {
    assertThat(Json.SCHEMA.getLogicalType().getName()).isEqualTo(Json.LOGICAL_TYPE);
    assertThat(Json.SCHEMA.getType()).isEqualTo(Schema.Type.STRING);
  }

  @Test
  public void testNumber() {
    assertThat(Number.SCHEMA.getLogicalType().getName()).isEqualTo(Number.LOGICAL_TYPE);
    assertThat(Number.SCHEMA.getType()).isEqualTo(Schema.Type.STRING);
  }

  @Test
  public void testTimeIntervalMicros() {
    assertThat(TimeIntervalMicros.SCHEMA.getLogicalType().getName())
        .isEqualTo(TimeIntervalMicros.LOGICAL_TYPE);
    assertThat(TimeIntervalMicros.SCHEMA.getType()).isEqualTo(Schema.Type.LONG);
  }
}
