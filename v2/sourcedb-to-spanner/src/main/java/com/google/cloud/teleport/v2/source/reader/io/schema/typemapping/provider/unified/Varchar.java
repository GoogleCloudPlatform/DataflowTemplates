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
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.ArrayUtils;

/** Generates a Varchar Avro logical type. */
public final class Varchar implements UnifiedTypeMapping {

  public static final String LENGTH_PROP_NAME = "length";
  public static final String LOGICAL_TYPE = "varchar";

  @Override
  public Schema getSchema(Long[] mods, Long[] arrayBounds) {
    Preconditions.checkArgument(ArrayUtils.isEmpty(arrayBounds), "Arrays are not supported");
    Preconditions.checkArgument(mods != null, "Varchar type needs length");
    Preconditions.checkArgument(mods.length == 1, "Varchar type needs length");

    Schema schema = new LogicalType(LOGICAL_TYPE).addToSchema(SchemaBuilder.builder().stringType());
    schema.addProp(LENGTH_PROP_NAME, mods[0]);
    return schema;
  }
}
