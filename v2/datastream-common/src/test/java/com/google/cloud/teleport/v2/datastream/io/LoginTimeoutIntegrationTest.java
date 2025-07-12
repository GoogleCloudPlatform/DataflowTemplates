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
package com.google.cloud.teleport.v2.datastream.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.lang.reflect.Field;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration test to verify loginTimeout functionality in CdcJdbcIO.DataSourceConfiguration. This
 * test validates that the loginTimeout parameter is properly set on the DataSource.
 */
@RunWith(JUnit4.class)
public class LoginTimeoutIntegrationTest {

  @Test
  public void testLoginTimeoutIsSetOnDataSource() throws SQLException {
    // Create a DataSourceConfiguration with loginTimeout
    CdcJdbcIO.DataSourceConfiguration config =
        CdcJdbcIO.DataSourceConfiguration.create(
                "org.h2.Driver", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1")
            .withUsername("sa")
            .withPassword("")
            .withLoginTimeout(45);

    // Build the DataSource
    DataSource dataSource = config.buildDatasource();

    // Verify that the DataSource is created and is a BasicDataSource
    assertNotNull("DataSource should not be null", dataSource);
    assertEquals(
        "DataSource should be a BasicDataSource", BasicDataSource.class, dataSource.getClass());

    // Verify that the loginTimeout is set correctly in connection properties
    BasicDataSource basicDataSource = (BasicDataSource) dataSource;
    try {
      Field connectionPropertiesField =
          BasicDataSource.class.getDeclaredField("connectionProperties");
      connectionPropertiesField.setAccessible(true);
      java.util.Properties connectionProperties =
          (java.util.Properties) connectionPropertiesField.get(basicDataSource);
      assertNotNull("Connection properties should not be null", connectionProperties);
      assertEquals(
          "Login timeout should be set to 45 seconds in connection properties",
          "45",
          connectionProperties.getProperty("loginTimeout"));
    } catch (Exception e) {
      throw new RuntimeException("Failed to access connectionProperties field", e);
    }
  }

  @Test
  public void testDefaultLoginTimeoutWhenNotSet() throws SQLException {
    // Create a DataSourceConfiguration without loginTimeout
    CdcJdbcIO.DataSourceConfiguration config =
        CdcJdbcIO.DataSourceConfiguration.create(
                "org.h2.Driver", "jdbc:h2:mem:testdb2;DB_CLOSE_DELAY=-1")
            .withUsername("sa")
            .withPassword("");

    // Build the DataSource
    DataSource dataSource = config.buildDatasource();

    // Verify that the DataSource is created
    assertNotNull("DataSource should not be null", dataSource);
    BasicDataSource basicDataSource = (BasicDataSource) dataSource;

    // Verify that no loginTimeout is set in connection properties when not specified
    try {
      Field connectionPropertiesField =
          BasicDataSource.class.getDeclaredField("connectionProperties");
      connectionPropertiesField.setAccessible(true);
      java.util.Properties connectionProperties =
          (java.util.Properties) connectionPropertiesField.get(basicDataSource);
      // Connection properties should be null or empty when no loginTimeout is set
      if (connectionProperties != null) {
        assertEquals(
            "Connection properties should be empty when no loginTimeout is set",
            0,
            connectionProperties.size());
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to access connectionProperties field", e);
    }
  }
}
