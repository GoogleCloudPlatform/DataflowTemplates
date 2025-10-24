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

public interface IDao<T> {
  /**
   * Executes a given write statement against the data source.
   *
   * @param statement Query statement.
   * @throws Exception If there is an error executing the statement.
   */
  void write(T statement) throws Exception;

  /**
   * Executes a given write statement against the data source then calls the commitCheck callback
   * function. The transaction will be committed only if the callback function returns true. In any
   * other case including the case where callback returned true but then the transaction could not
   * be committed, an exception will be thrown.
   *
   * @param sqlStatement Query statement.
   * @param commitCheck Callback function which will be executed and checked before committing the
   *     transaction.
   * @throws Exception If the sqlStatement could not be successfully committed.
   */
  void writeAndCheck(String sqlStatement, TransactionalCheck commitCheck) throws Exception;
}
