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
package com.google.cloud.teleport.v2.source.reader.io.schema.typemapping;

import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.avro.Schema;

/**
 * Interface to convert a source schema to {@link Schema Avro Schema} implemented by providers of
 * various source types, like {@link
 * com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.MysqlMappingProvider
 * MysqlMappingProvider}.
 */
public interface UnifiedTypeMapping extends Serializable {

  /**
   * Convert the Source Schema.
   *
   * @param mods Parameters like scale, precision are passed through mods. Refer to {@link
   *     com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType
   *     SourceColumnType.mods} for more details.
   * @param arrayBounds Bounds of array types.
   * @return Schema - {@link Schema Avro Schema}
   */
  Schema getSchema(@Nullable Long[] mods, @Nullable Long[] arrayBounds);
}
