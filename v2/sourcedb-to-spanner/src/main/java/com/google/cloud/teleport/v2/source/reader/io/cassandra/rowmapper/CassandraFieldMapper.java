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

import com.datastax.driver.core.Row;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.avro.Schema;

@AutoValue
public abstract class CassandraFieldMapper<T> implements Serializable {

  public static CassandraFieldMapper<?> create(
      CassandraRowValueExtractor<?> rowValueExtractor, CassandraRowValueMapper<?> rowValueMapper) {
    return new AutoValue_CassandraFieldMapper(rowValueExtractor, rowValueMapper);
  }

  public Object mapValue(Row row, String fieldName, Schema fieldSchema) {
    T extractedValue = rowValueExtractor().extract(row, fieldName);
    if (extractedValue == null) {
      return null;
    }
    Object avroValue = rowValueMapper().map(extractedValue, fieldSchema);
    return avroValue;
  }

  abstract CassandraRowValueExtractor<T> rowValueExtractor();

  abstract CassandraRowValueMapper<T> rowValueMapper();
}
