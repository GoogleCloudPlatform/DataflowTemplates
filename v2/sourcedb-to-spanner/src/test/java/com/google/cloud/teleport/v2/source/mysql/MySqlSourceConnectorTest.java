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
package com.google.cloud.teleport.v2.source.mysql;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MySqlSourceConnector}. */
@RunWith(JUnit4.class)
public class MySqlSourceConnectorTest {

  private final MySqlSourceConnector connector = new MySqlSourceConnector();

  @Test
  public void testGetJdbcUrl_withHostPort() {
    String url = connector.getJdbcUrl("localhost", 3306, "testDB", null, "useSSL=true", 42, null);
    assertThat(url)
        .isEqualTo(
            "jdbc:mysql://localhost:3306/testDB?useSSL=true&allowMultiQueries=true&autoReconnect=true&maxReconnects=10&useCursorFetch=true");
  }

  @Test
  public void testGetJdbcUrl_withPrebuiltUrl_nullFetchSize() {
    String url =
        connector.getJdbcUrl(
            null, 0, null, null, null, null, "jdbc:mysql://localhost:3306/testDB?useSSL=true");
    assertThat(url)
        .isEqualTo(
            "jdbc:mysql://localhost:3306/testDB?useSSL=true&allowMultiQueries=true&autoReconnect=true&maxReconnects=10&useCursorFetch=true");
  }

  @Test
  public void testGetJdbcUrl_withPrebuiltUrl_zeroFetchSize() {
    String url =
        connector.getJdbcUrl(
            null, 0, null, null, null, 0, "jdbc:mysql://localhost:3306/testDB?useSSL=true");
    assertThat(url)
        .isEqualTo(
            "jdbc:mysql://localhost:3306/testDB?useSSL=true&allowMultiQueries=true&autoReconnect=true&maxReconnects=10");
  }

  @Test
  public void testGetJdbcUrl_withPrebuiltUrl_positiveFetchSize() {
    String url =
        connector.getJdbcUrl(
            null, 0, null, null, null, 100, "jdbc:mysql://localhost:3306/testDB?useSSL=true");
    assertThat(url)
        .isEqualTo(
            "jdbc:mysql://localhost:3306/testDB?useSSL=true&allowMultiQueries=true&autoReconnect=true&maxReconnects=10&useCursorFetch=true");
  }
}
