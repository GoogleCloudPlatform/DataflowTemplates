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
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Wraps a simple {@link Schema Avro Schema} into the {@link UnifiedTypeMapping} interface. The
 * {@code SimpleUnifiedTypeMapping} implementation ignores the {@link
 * com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType mods}. In case you need
 * to consider the mods in your mapping, you would have to implement the {@link UnifiedTypeMapping}
 * interface separately, for example {@link Varchar}, or {@link Decimal}.
 */
@AutoValue
abstract class SimpleUnifiedTypeMapping implements UnifiedTypeMapping {

  public static SimpleUnifiedTypeMapping create(Schema schema) {
    return new AutoValue_SimpleUnifiedTypeMapping(schema);
  }

  public abstract Schema schema();

  @Override
  public Schema getSchema(@Nullable Long[] mods, @Nullable Long[] arrayBounds) {
    Preconditions.checkArgument(ArrayUtils.isEmpty(arrayBounds), "Arrays are not supported");
    return this.schema();
  }
}
