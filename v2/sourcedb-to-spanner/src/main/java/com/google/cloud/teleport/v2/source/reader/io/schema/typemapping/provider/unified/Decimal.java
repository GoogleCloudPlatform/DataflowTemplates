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

import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapping;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Generates a <a href=https://avro.apache.org/docs/1.8.2/spec.html#Decimal>Decimal</a> Avro Type.
 */
final class Decimal implements UnifiedTypeMapping {

  @Override
  public Schema getSchema(@Nullable Long[] mods, @Nullable Long[] arrayBounds) {
    Preconditions.checkArgument(ArrayUtils.isEmpty(arrayBounds), "Arrays are not supported");
    Preconditions.checkArgument(mods != null, "Decimal type needs precision and scale");
    Preconditions.checkArgument(mods.length == 2, "Decimal type needs precision and scale");
    return LogicalTypes.decimal(mods[0].intValue(), mods[1].intValue())
        .addToSchema(Schema.create(Schema.Type.BYTES));
  }
}
