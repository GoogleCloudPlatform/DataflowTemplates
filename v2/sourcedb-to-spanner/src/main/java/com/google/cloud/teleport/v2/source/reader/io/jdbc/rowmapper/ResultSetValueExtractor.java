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
import javax.annotation.Nullable;

/**
 * An interface to represent functions that extract values from resultSet.
 *
 * @param <T> type of the value extracted.
 */
public interface ResultSetValueExtractor<T extends Object> extends Serializable {

  /**
   * Extract the requested field from the result set.
   *
   * @param rs resultSet.
   * @param fieldName name of the field to extract.
   * @return extracted value.
   * @throws SQLException Any exception thrown by ResultSet API. Typically indicated a change in
   *     schema during migration.
   */
  @Nullable
  T extract(ResultSet rs, String fieldName) throws SQLException;
}
