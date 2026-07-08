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
package com.google.cloud.teleport.v2.templates.dbutils.dml;

import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;

/**
 * Interface for generating DML statements.
 *
 * <p>This interface provides a contract for implementing classes to define the logic for generating
 * DML statements, such as INSERT, UPDATE, and DELETE, based on a provided {@link
 * DMLGeneratorRequest}.
 *
 * <p>Classes implementing this interface should handle the construction of DML statements tailored
 * to the specific requirements of the target database.
 */
public interface IDMLGenerator {
  /**
   * Generates a DML statement based on the provided {@link DMLGeneratorRequest}.
   *
   * @param dmlGeneratorRequest the request containing necessary information to construct the DML
   *     statement, including modification type, table schema, new values, and key values.
   * @return a {@link DMLGeneratorResponse} object containing the generated DML statement.
   */
  DMLGeneratorResponse getDMLStatement(DMLGeneratorRequest dmlGeneratorRequest);
}
