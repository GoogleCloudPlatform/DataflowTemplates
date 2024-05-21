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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.avro.Schema;

/**
 * Wrap the {@link ResultSetValueExtractor} and {@link ResultSetValueMapper} to map a given field of
 * {@link ResultSet}.
 *
 * @param <T>
 */
public class JdbcValueMapper<T extends Object> implements Serializable {

  private ResultSetValueExtractor<T> valueExtractor;
  private ResultSetValueMapper<T> valueMapper;

  /**
   * Construct the {@link JdbcValueMapper}.
   *
   * @param resultSetValueExtractor Extractor to extract the desired field from resultType.
   * @param resultSetValueMapper Mapper for the extracted value.
   */
  public JdbcValueMapper(
      ResultSetValueExtractor<?> resultSetValueExtractor,
      ResultSetValueMapper<?> resultSetValueMapper) {
    this.valueExtractor = (ResultSetValueExtractor<T>) resultSetValueExtractor;
    this.valueMapper = (ResultSetValueMapper<T>) resultSetValueMapper;
  }

  /**
   * Map a given field of {@link ResultSet} to an object as per the {@link Schema Avro Schema}.
   *
   * @param rs resultSet.
   * @param fieldName name of the field to map.
   * @param fieldSchema {@link Schema Avro Schema} of the field.
   * @return Mapped value.
   * @throws SQLException - Exception while extracting value from {@link ResultSet}. Typically,
   *     indicates change in source schema during migration.
   */
  public Object mapValue(ResultSet rs, String fieldName, Schema fieldSchema) throws SQLException {
    var extractedValue = valueExtractor.extract(rs, fieldName);
    if (extractedValue == null || rs.wasNull()) {
      return null;
    }
    return valueMapper.map(extractedValue, fieldSchema);
  }

  public static final JdbcValueMapper<?> UNSUPPORTED =
      new JdbcValueMapper<>((rs, field) -> null, (value, schema) -> null);
}
