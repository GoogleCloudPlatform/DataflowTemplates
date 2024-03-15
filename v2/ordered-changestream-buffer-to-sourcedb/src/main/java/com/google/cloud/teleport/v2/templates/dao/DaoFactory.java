/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.dao;

import com.google.cloud.teleport.v2.templates.common.ProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DaoFactory, currently only supports MySql. */
public class DaoFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DaoFactory.class);

  public static Dao getDao(ProcessingContext taskContext) throws IllegalArgumentException {
    LOG.info(String.format("Getting the %s DAO", taskContext.getSourceDbType()));
    if ("mysql".equals(taskContext.getSourceDbType())) {
      String sqlUrl =
          "jdbc:mysql://"
              + taskContext.getShard().getHost()
              + ":"
              + taskContext.getShard().getPort()
              + "/"
              + taskContext.getShard().getDbName();
      return new MySqlDao(
          sqlUrl,
          taskContext.getShard().getUserName(),
          taskContext.getShard().getPassword(),
          taskContext.getShard().getLogicalShardId(),
          taskContext.getEnableSourceDbSsl(),
          taskContext.getEnableSourceDbSslValidation(),
          "");
    } else if ("postgresql".equals(taskContext.getSourceDbType())) {
      String sqlUrl =
          "jdbc:postgresql://"
              + taskContext.getShard().getHost()
              + ":"
              + taskContext.getShard().getPort()
              + "/"
              + taskContext.getShard().getDbName();
      return new PostgreSqlDao(
          sqlUrl,
          taskContext.getShard().getUserName(),
          taskContext.getShard().getPassword(),
          taskContext.getShard().getLogicalShardId(),
          taskContext.getEnableSourceDbSsl(),
          taskContext.getEnableSourceDbSslValidation(),
          "");
    }
    throw new IllegalArgumentException(
        String.format("No DAO found for %s", taskContext.getSourceDbType()));
  }
}
