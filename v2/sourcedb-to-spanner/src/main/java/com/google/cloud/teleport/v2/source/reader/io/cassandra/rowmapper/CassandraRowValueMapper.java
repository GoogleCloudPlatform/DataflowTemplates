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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper;

import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.checkerframework.checker.nullness.qual.NonNull;

public interface CassandraRowValueMapper<T extends Object> extends Serializable {

  /**
   * Map the extracted value to an object accepted by {@link
   * org.apache.avro.generic.GenericRecordBuilder#set(Field, Object)} as per the schema of the
   * field.
   *
   * @param value extracted value.
   * @param schema Avro Schema.
   * @return mapped object.
   */
  Object map(@NonNull T value, Schema schema);
}
