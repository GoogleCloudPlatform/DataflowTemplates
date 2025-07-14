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
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.Map;
import org.apache.avro.Schema;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Mapper for Mapping the values of `map` fileds in a Cassandra Row.
 *
 * @param <K> key type
 * @param <V> value type.
 */
@AutoValue
public abstract class CassandraRowValueMapMapper<K, V>
    implements CassandraRowValueMapper<Map<K, V>> {

  public static <K, V> CassandraRowValueMapMapper<K, V> create(
      CassandraRowValueMapper<K> keyRowValueMapper,
      CassandraRowValueMapper<V> valueRowValueMapper,
      Schema keySchema,
      Schema valueSchema) {
    return new AutoValue_CassandraRowValueMapMapper<>(
        keyRowValueMapper, valueRowValueMapper, keySchema, valueSchema);
  }

  abstract CassandraRowValueMapper<K> keyRowValueMapper();

  abstract CassandraRowValueMapper<V> valueRowValueMapper();

  abstract Schema keySchema();

  abstract Schema valueSchema();

  /**
   * Map the extracted value of the map to an Avro JSON
   *
   * <p>Note: An Avro Json is just an annotated String. Both the key and value of the map are
   * represented as a string.
   *
   * @param values extracted value collection.
   * @param schema Avro Schema.
   * @return mapped object.
   */
  @Override
  public Object map(@NonNull Map<K, V> values, Schema schema) {
    JsonObject jsonObject = new JsonObject();
    values.forEach(
        (k, v) ->
            jsonObject.add(
                keyRowValueMapper().map(k, keySchema()).toString(),
                new JsonPrimitive(valueRowValueMapper().map(v, valueSchema()).toString())));
    return jsonObject.toString();
  }
}
