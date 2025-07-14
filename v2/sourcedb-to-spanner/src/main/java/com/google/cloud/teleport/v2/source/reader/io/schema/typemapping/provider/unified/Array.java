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

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapping;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.avro.Schema;

/**
 * Generates a <a href=https://avro.apache.org/docs/1.8.2/spec.html#Decimal>Decimal</a> Avro Type.
 */
@AutoValue
abstract class Array implements UnifiedTypeMapping, Serializable {

  public static Array create(UnifiedTypeMapping mapping) {
    return new AutoValue_Array(mapping);
  }

  abstract UnifiedTypeMapping mapping();

  @Override
  public Schema getSchema(@Nullable Long[] mods, @Nullable Long[] arrayBounds) {
    return Schema.createArray(mapping().getSchema(mods, arrayBounds));
  }
}
