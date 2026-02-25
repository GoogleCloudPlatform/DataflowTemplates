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
      CassandraRowValueExtractor<?> rowValueExtractor,
      CassandraRowValueExtractorV4<?> rowValueExtractorV4,
      CassandraRowValueMapper<?> rowValueMapper,
      CassandraRowValueMapper<?> rowValueMapperV4) {
    return new AutoValue_CassandraFieldMapper(
        rowValueExtractor, rowValueExtractorV4, rowValueMapper, rowValueMapperV4);
  }

  /**
   * Map the extracted value to an object accepted by {@link
   * org.apache.avro.generic.GenericRecordBuilder#set(Field, Object)} as per the schema of the
   * field.
   *
   * @param row row derived from {@link ResultSet}.
   * @param fieldName name of the field to extract.
   * @param fieldSchema schema of the field.
   * @return mapped object.
   */
  public Object mapValue(Row row, String fieldName, Schema fieldSchema) {

    /*
     * Some of the methods in {@link Row} return non-null defaults for null values.
     * For example {@link Row#getLong} returns 0 for null values.
     * This method ensures that a null check is performed before calling the methods in {@link Row}
     * which might return non-null defaults for null values.
     */
    if (row.isNull(fieldName)) {
      return null;
    }
    T extractedValue = rowValueExtractor().extract(row, fieldName);
    if (extractedValue == null) {
      return null;
    }
    Object avroValue = rowValueMapper().map(extractedValue, fieldSchema);
    return avroValue;
  }

  /**
   * Map the extracted value to an object accepted by {@link
   * org.apache.avro.generic.GenericRecordBuilder#set(Field, Object)} as per the schema of the
   * field.
   *
   * @param row row derived from {@link com.datastax.oss.driver.api.core.cql.ResultSet}.
   * @param fieldName name of the field to extract.
   * @param fieldSchema schema of the field.
   * @return mapped object.
   */
  public Object mapValueV4(
      com.datastax.oss.driver.api.core.cql.Row row, String fieldName, Schema fieldSchema) {
    /*
     * Some of the methods in {@link Row} return non-null defaults for null values.
     * For example {@link Row#getLong} returns 0 for null values.
     * This method ensures that a null check is performed before calling the methods in {@link Row}
     * which might return non-null defaults for null values.
     */
    if (row.isNull(fieldName)) {
      return null;
    }
    T extractedValue = rowValueExtractorV4().extract(row, fieldName);
    if (extractedValue == null) {
      return null;
    }
    Object avroValue = rowValueMapperV4().map(extractedValue, fieldSchema);
    return avroValue;
  }

  public abstract CassandraRowValueExtractor<T> rowValueExtractor();

  public abstract CassandraRowValueExtractorV4<T> rowValueExtractorV4();

  public abstract CassandraRowValueMapper<T> rowValueMapper();

  public abstract CassandraRowValueMapper<T> rowValueMapperV4();
}
