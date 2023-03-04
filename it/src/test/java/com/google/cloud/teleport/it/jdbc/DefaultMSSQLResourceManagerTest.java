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
package com.google.cloud.teleport.it.jdbc;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Integration tests for {@link DefaultMSSQLResourceManagerTest}. */
@RunWith(JUnit4.class)
public class DefaultMSSQLResourceManagerTest<
    T extends DefaultMSSQLResourceManager.DefaultMSSQLServerContainer<T>> {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private T container;

  private DefaultMSSQLResourceManager testManager;

  private static final String TEST_ID = "test_id";
  private static final String DATABASE_NAME = "database";
  private static final String HOST = "localhost";
  private static final int MAPPED_PORT = 1234;

  @Before
  public void setUp() {
    when(container.withUsername(any())).thenReturn(container);
    when(container.withPassword(any())).thenReturn(container);
    when(container.withDatabaseName(anyString())).thenReturn(container);
    when(container.getDatabaseName()).thenReturn(DATABASE_NAME);
    testManager =
        new DefaultMSSQLResourceManager(
            container, new DefaultMSSQLResourceManager.Builder(TEST_ID));
  }

  @Test
  public void testGetJDBCPortReturnsCorrectValue() {
    assertThat(testManager.getJDBCPort())
        .isEqualTo(DefaultMSSQLResourceManager.DefaultMSSQLServerContainer.MS_SQL_SERVER_PORT);
  }

  @Test
  public void testGetUriShouldReturnCorrectValue() {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(
            DefaultMSSQLResourceManager.DefaultMSSQLServerContainer.MS_SQL_SERVER_PORT))
        .thenReturn(MAPPED_PORT);
    assertThat(testManager.getUri())
        .matches(
            "jdbc:sqlserver://"
                + HOST
                + ":"
                + MAPPED_PORT
                + ";DatabaseName="
                + DATABASE_NAME
                + ";encrypt=true;trustServerCertificate=true;");
  }
}
