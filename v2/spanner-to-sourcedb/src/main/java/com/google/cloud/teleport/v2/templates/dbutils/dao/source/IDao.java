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
package com.google.cloud.teleport.v2.templates.dbutils.dao.source;

import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;

public interface IDao {
  /**
   * Executes a given write dmlGeneratorResponse against the data source then calls the
   * transactionalCheck callback function (if not null). The transaction will be committed only if
   * the callback function did not throw any exception. In any other case, an exception will be
   * thrown.
   *
   * @param dmlGeneratorResponse Query dmlGeneratorResponse.
   * @param transactionalCheck Callback function which will be executed and checked before
   *     committing the transaction.
   * @throws Exception If the sqlStatement could not be successfully committed.
   */
  void write(DMLGeneratorResponse dmlGeneratorResponse, TransactionalCheck transactionalCheck)
      throws Exception;

  /**
   * Executes a given write dmlGeneratorResponse against the data source under a specific
   * transaction context. If the transaction context is null, it delegates to {@link #write(Object,
   * TransactionalCheck)}.
   *
   * @param dmlGeneratorResponse Query dmlGeneratorResponse.
   * @param transactionalCheck Callback function which will be executed and checked before
   *     committing the transaction.
   * @param transactionContext The transaction context to execute the write under.
   * @throws Exception If the write could not be successfully executed.
   */
  default void write(
      DMLGeneratorResponse dmlGeneratorResponse,
      TransactionalCheck transactionalCheck,
      Object transactionContext)
      throws Exception {
    write(dmlGeneratorResponse, transactionalCheck);
  }
}
