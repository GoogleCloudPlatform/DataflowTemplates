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
import org.apache.beam.it.jdbc.OracleResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SharedOracleReverseITContainer {
  private static final Logger LOG = LoggerFactory.getLogger(SharedOracleReverseITContainer.class);

  private static OracleResourceManager instance;

  public static synchronized OracleResourceManager getInstance() {
    if (instance == null) {
      instance = OracleResourceManager.builder("oracle-rev-bulk-db").build();
      try {
        try (Connection systemConn =
                DriverManager.getConnection(instance.getUri(), "SYSTEM", instance.getPassword());
            Statement stmt = systemConn.createStatement()) {
          stmt.execute("GRANT DBA TO " + instance.getUsername());
          LOG.info("Successfully granted DBA to Testcontainers Oracle app user!");
        }
      } catch (Exception e) {
        LOG.warn("Failed to grant DBA using SYSTEM. CREATE USER might fail.", e);
      }
    }
    return instance;
  }
}
