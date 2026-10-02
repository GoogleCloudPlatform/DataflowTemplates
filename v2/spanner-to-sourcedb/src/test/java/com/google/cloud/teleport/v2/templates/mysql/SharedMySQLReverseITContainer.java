/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.mysql;

import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SharedMySQLReverseITContainer {
  private static final Logger LOG = LoggerFactory.getLogger(SharedMySQLReverseITContainer.class);
  private static MySQLResourceManager singletonBase;

  public static synchronized MySQLResourceManager createResourceManager(String testId) {
    if (singletonBase == null) {
      singletonBase = MySQLResourceManager.builder("mysql-reverse-base").build();
      LOG.info("Successfully started singleton MySQL TestContainer!");
    }

    MySQLResourceManager.Builder builder = MySQLResourceManager.builder(testId);
    builder.setUsername(singletonBase.getUsername()).setPassword(singletonBase.getPassword());
    builder.useStaticContainer();
    builder.setHost(singletonBase.getHost()).setPort(singletonBase.getPort());

    MySQLResourceManager isolatedRm = builder.build();

    try {
      singletonBase.runSQLUpdate("CREATE DATABASE " + isolatedRm.getDatabaseName());
      LOG.info(
          "Created isolated Database {} on shared MySQL container", isolatedRm.getDatabaseName());
    } catch (Exception e) {
      throw new RuntimeException("Failed to provision isolated test database", e);
    }

    return isolatedRm;
  }
}
