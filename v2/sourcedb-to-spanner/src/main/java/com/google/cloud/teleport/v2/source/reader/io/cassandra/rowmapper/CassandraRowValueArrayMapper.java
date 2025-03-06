/*
 * Copyright (C) 2025 Google LLC
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

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecordBuilder;
import org.checkerframework.checker.nullness.qual.NonNull;

@AutoValue
public abstract class CassandraRowValueArrayMapper<T>
    implements CassandraRowValueMapper<Iterable<T>> {

  public static <T> CassandraRowValueArrayMapper<T> create(
      CassandraRowValueMapper<T> rowValueMapper) {
    return new AutoValue_CassandraRowValueArrayMapper<>(rowValueMapper);
  }

  abstract CassandraRowValueMapper<T> rowValueMapper();

  /**
   * Map the extracted value to an object accepted by {@link GenericRecordBuilder#set(Field,
   * Object)} as per the schema of the field.
   *
   * @param values extracted value collection.
   * @param schema Avro Schema.
   * @return mapped object.
   */
  @Override
  public Object map(@NonNull Iterable<T> values, Schema schema) {
    ArrayList<Object> mapped = new ArrayList();
    values.forEach(v -> mapped.add(rowValueMapper().map(v, schema.getElementType())));
    return mapped;
  }
}
