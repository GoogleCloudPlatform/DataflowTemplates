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
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test cases for the {@link CdcJdbcIO} class. */
@RunWith(JUnit4.class)
public final class CdcJdbcIOTest {

  private static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
  private static final String URL = "jdbc:mysql://localhost:3306/test";
  private static final String USERNAME = "testuser";
  private static final String PASSWORD = "testpass";
  private static final Integer LOGIN_TIMEOUT = 30;

  @Test
  public void testDataSourceConfiguration_withLoginTimeout_staticValue() {
    CdcJdbcIO.DataSourceConfiguration config =
        CdcJdbcIO.DataSourceConfiguration.create(DRIVER_CLASS_NAME, URL)
            .withUsername(USERNAME)
            .withPassword(PASSWORD)
            .withLoginTimeout(LOGIN_TIMEOUT);

    assertNotNull(config.getLoginTimeout());
    assertEquals(LOGIN_TIMEOUT, config.getLoginTimeout().get());
  }

  @Test
  public void testDataSourceConfiguration_withLoginTimeout_valueProvider() {
    ValueProvider<Integer> loginTimeoutProvider =
        ValueProvider.StaticValueProvider.of(LOGIN_TIMEOUT);

    CdcJdbcIO.DataSourceConfiguration config =
        CdcJdbcIO.DataSourceConfiguration.create(DRIVER_CLASS_NAME, URL)
            .withUsername(USERNAME)
            .withPassword(PASSWORD)
            .withLoginTimeout(loginTimeoutProvider);

    assertNotNull(config.getLoginTimeout());
    assertEquals(LOGIN_TIMEOUT, config.getLoginTimeout().get());
  }

  @Test
  public void testDataSourceConfiguration_buildDatasource_withLoginTimeout() throws SQLException {
    CdcJdbcIO.DataSourceConfiguration config =
        CdcJdbcIO.DataSourceConfiguration.create(DRIVER_CLASS_NAME, URL)
            .withUsername(USERNAME)
            .withPassword(PASSWORD)
            .withLoginTimeout(LOGIN_TIMEOUT);

    DataSource dataSource = config.buildDatasource();

    assertNotNull(dataSource);
    assertEquals(BasicDataSource.class, dataSource.getClass());

    BasicDataSource basicDataSource = (BasicDataSource) dataSource;
    // BasicDataSource.getLoginTimeout() throws UnsupportedOperationException,
    // so we verify that loginTimeout is included in connection properties instead
    try {
      Field connectionPropertiesField =
          BasicDataSource.class.getDeclaredField("connectionProperties");
      connectionPropertiesField.setAccessible(true);
      java.util.Properties connectionProperties =
          (java.util.Properties) connectionPropertiesField.get(basicDataSource);
      assertNotNull(connectionProperties);
      assertEquals(LOGIN_TIMEOUT.toString(), connectionProperties.getProperty("loginTimeout"));
    } catch (Exception e) {
      throw new RuntimeException("Failed to access connectionProperties field", e);
    }
    assertEquals(DRIVER_CLASS_NAME, basicDataSource.getDriverClassName());
    assertEquals(URL, basicDataSource.getUrl());
    assertEquals(USERNAME, basicDataSource.getUsername());
    assertEquals(PASSWORD, basicDataSource.getPassword());
  }

  @Test
  public void testDataSourceConfiguration_buildDatasource_withoutLoginTimeout()
      throws SQLException {
    CdcJdbcIO.DataSourceConfiguration config =
        CdcJdbcIO.DataSourceConfiguration.create(DRIVER_CLASS_NAME, URL)
            .withUsername(USERNAME)
            .withPassword(PASSWORD);

    DataSource dataSource = config.buildDatasource();

    assertNotNull(dataSource);
    assertEquals(BasicDataSource.class, dataSource.getClass());

    BasicDataSource basicDataSource = (BasicDataSource) dataSource;
    // When no loginTimeout is set, connection properties should be null or empty
    try {
      Field connectionPropertiesField =
          BasicDataSource.class.getDeclaredField("connectionProperties");
      connectionPropertiesField.setAccessible(true);
      java.util.Properties connectionProperties =
          (java.util.Properties) connectionPropertiesField.get(basicDataSource);
      // Connection properties should be null or empty when no loginTimeout and no connection
      // properties are set
      if (connectionProperties != null) {
        assertEquals(0, connectionProperties.size());
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to access connectionProperties field", e);
    }
    assertEquals(DRIVER_CLASS_NAME, basicDataSource.getDriverClassName());
    assertEquals(URL, basicDataSource.getUrl());
    assertEquals(USERNAME, basicDataSource.getUsername());
    assertEquals(PASSWORD, basicDataSource.getPassword());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDataSourceConfiguration_withLoginTimeout_nullValue() {
    CdcJdbcIO.DataSourceConfiguration.create(DRIVER_CLASS_NAME, URL)
        .withLoginTimeout((ValueProvider<Integer>) null);
  }

  @Test
  public void testDataSourceConfiguration_withLoginTimeout_nullValueProvider() throws SQLException {
    ValueProvider<Integer> nullProvider = ValueProvider.StaticValueProvider.of(null);

    CdcJdbcIO.DataSourceConfiguration config =
        CdcJdbcIO.DataSourceConfiguration.create(DRIVER_CLASS_NAME, URL)
            .withLoginTimeout(nullProvider);

    DataSource dataSource = config.buildDatasource();

    assertNotNull(dataSource);
    assertEquals(BasicDataSource.class, dataSource.getClass());

    BasicDataSource basicDataSource = (BasicDataSource) dataSource;
    // Note: BasicDataSource.getLoginTimeout() throws UnsupportedOperationException
    // so we cannot directly verify the login timeout value
  }
}
