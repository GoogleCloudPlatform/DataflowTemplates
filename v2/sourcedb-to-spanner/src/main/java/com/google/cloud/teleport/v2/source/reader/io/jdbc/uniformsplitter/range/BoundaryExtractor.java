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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Interface to extract Boundary from a given resultSet for the boundary query of the form {@code
 * SELECT MIN(ColumnName), MAX(ColumnName) from Table}.
 *
 * <p>Note:
 *
 * <p>Implementations must handle the SQLException thrown.
 */
public interface BoundaryExtractor<T extends Serializable> extends Serializable {

  /**
   * Extract Boundary from a given resultSet for the boundary query of the form {@code SELECT
   * MIN(columnName), MAX(columnName) from Table}.
   *
   * @param partitionColumn details of the partition column.
   * @param resultSet result set.
   * @return extracted boundary.
   * @throws SQLException thrown by the resultSet api.
   */
  Boundary<T> getBoundary(
      PartitionColumn partitionColumn,
      ResultSet resultSet,
      @Nullable BoundaryTypeMapper boundaryTypeMapper)
      throws SQLException;
}
