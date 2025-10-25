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
package com.google.cloud.teleport.v2.templates.dbutils.dao.source;

/**
 * A functional interface for performing a check within a transaction. The check must return 'true'
 * to commit. 'False' or exception would result in rollback. This is used in IDao.
 */
@FunctionalInterface
public interface TransactionalCheck {
  /**
   * Performs the check.
   *
   * @return true to proceed with commit, false to trigger a rollback.
   * @throws Exception if the check itself fails with an Exception.
   */
  boolean check() throws Exception;
}
