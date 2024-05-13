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
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/** Generates a null type for mapping unsupported type. */
public final class Unsupported implements UnifiedTypeMapping {
  public static final String UNSUPPORTED_LOGICAL_TYPE_NAME = "unsupported";
  private static final Schema schema =
      new LogicalType(UNSUPPORTED_LOGICAL_TYPE_NAME)
          .addToSchema(SchemaBuilder.builder().nullType());

  @Override
  public Schema getSchema(Long[] mods, Long[] arrayBounds) {
    return schema;
  }

  /**
   * Tests is a given Schema is of the type Unsupported.
   *
   * @param schema Schema to test
   * @return true if schema is not supported. false otherwise.
   */
  public boolean isUnsupported(Schema schema) {
    return schema.equals(this.schema);
  }
}
