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
import com.datastax.oss.driver.api.core.cql.ResultSet;
import java.io.Serializable;
import javax.annotation.Nullable;

public interface CassandraRowValueExtractor<T extends Object> extends Serializable {

  /**
   * Extract the requested field from the result set.
   *
   * @param row row derived from {@link ResultSet}.
   * @param fieldName name of the field to extract.
   * @return extracted value.
   * @throws IllegalArgumentException - thrown from Cassandra driver for invalid names.
   */
  @Nullable
  T extract(Row row, String fieldName) throws IllegalArgumentException;
}
