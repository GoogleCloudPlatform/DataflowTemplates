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
package com.google.cloud.teleport.v2.templates.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.it.jdbc.OracleResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SharedOracleReverseITContainer {
  private static final Logger LOG = LoggerFactory.getLogger(SharedOracleReverseITContainer.class);
  private static final int POOL_SIZE = 4;
  private static final OracleResourceManager[] instances = new OracleResourceManager[POOL_SIZE];
  private static final AtomicInteger nextIdx = new AtomicInteger(0);
  private static final ConcurrentHashMap<String, OracleResourceManager> classToInstance =
      new ConcurrentHashMap<>();

  public static synchronized OracleResourceManager getInstance() {
    String callerClass = "unknown";
    for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
      if (!e.getClassName().contains("SharedOracleReverseITContainer")
          && !e.getClassName().contains("java.lang.Thread")) {
        callerClass = e.getClassName();
        break;
      }
    }

    return classToInstance.computeIfAbsent(
        callerClass,
        k -> {
          int idx = Math.abs(nextIdx.getAndIncrement() % POOL_SIZE);
          if (instances[idx] == null) {
            LOG.info("Spinning up Shared Oracle Container #{}", idx);
            OracleResourceManager newInstance =
                OracleResourceManager.builder("oracle-rev-bulk-db-" + idx).build();
            try {
              try (Connection systemConn =
                      DriverManager.getConnection(
                          newInstance.getUri(), "SYSTEM", newInstance.getPassword());
                  Statement stmt = systemConn.createStatement()) {
                stmt.execute("GRANT DBA TO " + newInstance.getUsername());
                LOG.info("Successfully granted DBA to test user in Container #{}", idx);
              }
            } catch (Exception e) {
              LOG.warn("Failed to grant DBA using SYSTEM in Container #{}", idx, e);
            }
            instances[idx] = newInstance;
          }
          LOG.info("Assigning Oracle Container #{} to test class: {}", idx, k);
          return instances[idx];
        });
  }
}
