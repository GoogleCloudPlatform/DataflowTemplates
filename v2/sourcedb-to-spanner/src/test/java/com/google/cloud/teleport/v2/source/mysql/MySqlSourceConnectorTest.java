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
package com.google.cloud.teleport.v2.source.mysql;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link MySqlSrcToSpSourceConnector}. */
@RunWith(JUnit4.class)
public class MySqlSourceConnectorTest {

  private final MySqlSrcToSpSourceConnector connector = new MySqlSrcToSpSourceConnector();

  @Test
  public void testGetSourceType() {
    assertThat(connector.getSourceType()).isEqualTo(Constants.MYSQL_SOURCE_TYPE);
  }

  @Test
  public void testGetJdbcUrl_constructsUrl() {
    String url = connector.getJdbcUrl("localhost", 3306, "test_db", null, null, null);
    assertThat(url).startsWith("jdbc:mysql://localhost:3306/test_db");
    // Should contain defaults
    assertThat(url).contains("allowMultiQueries=true");
    assertThat(url).contains("autoReconnect=true");
    assertThat(url).contains("maxReconnects=10");
    // Should contain cursor fetch by default
    assertThat(url).contains("useCursorFetch=true");
  }

  @Test
  public void testGetJdbcUrl_withConnectionProperties() {
    String url = connector.getJdbcUrl("localhost", 3306, "test_db", "param1=value1", null, null);
    assertThat(url).startsWith("jdbc:mysql://localhost:3306/test_db?param1=value1");
    assertThat(url).contains("allowMultiQueries=true");
    assertThat(url).contains("useCursorFetch=true");
  }

  @Test
  public void testGetJdbcUrl_withFetchSizeNull_enablesCursorFetch() {
    String url = connector.getJdbcUrl("localhost", 3306, "test_db", null, null, null);
    assertThat(url).contains("useCursorFetch=true");
  }

  @Test
  public void testGetJdbcUrl_withFetchSizePositive_enablesCursorFetch() {
    String url = connector.getJdbcUrl("localhost", 3306, "test_db", null, null, 42);
    assertThat(url).contains("useCursorFetch=true");
  }

  @Test
  public void testGetJdbcUrl_withFetchSizeZero_disablesCursorFetch() {
    String url = connector.getJdbcUrl("localhost", 3306, "test_db", null, null, 0);
    assertThat(url).doesNotContain("useCursorFetch");
  }

  @Test
  public void testGetJdbcUrl_preservesExistingParams() {
    String url = connector.getJdbcUrl("localhost", 3306, "test_db", "param=value", null, null);
    assertThat(url).startsWith("jdbc:mysql://localhost:3306/test_db?param=value");
    assertThat(url).contains("allowMultiQueries=true");
    assertThat(url).contains("useCursorFetch=true");
  }

  @Test
  public void testGetJdbcUrl_withFetchSizeMinusOne_enablesCursorFetch() {
    // Note: In the builder, -1 is normalized to null BEFORE calling
    // mysqlSetCursorModeIfNeeded,
    // but here we test the method directly. If we pass -1 directly (if it were
    // possible),
    // it would be treated as != 0, so it would enable cursor mode.
    // However, the main propagation test testFetchSizeMinusOneBehavesLikeNull
    // covers the normalization.
    String url = connector.getJdbcUrl("localhost", 3306, "test_db", null, null, -1);
    assertThat(url).contains("useCursorFetch=true");
  }

  @Test
  public void testMysqlSetCursorModeIfNeeded_withNullFetchSize() {
    String url = connector.mysqlSetCursorModeIfNeeded("jdbc:mysql://localhost:3306/db", null);
    assertThat(url).isEqualTo("jdbc:mysql://localhost:3306/db?useCursorFetch=true");
  }

  @Test
  public void testMysqlSetCursorModeIfNeeded_withPositiveFetchSize() {
    String url = connector.mysqlSetCursorModeIfNeeded("jdbc:mysql://localhost:3306/db", 50000);
    assertThat(url).isEqualTo("jdbc:mysql://localhost:3306/db?useCursorFetch=true");
  }

  @Test
  public void testMysqlSetCursorModeIfNeeded_withZeroFetchSize() {
    String url = connector.mysqlSetCursorModeIfNeeded("jdbc:mysql://localhost:3306/db", 0);
    assertThat(url).isEqualTo("jdbc:mysql://localhost:3306/db");
  }

  @Test
  public void testMysqlSetCursorModeIfNeeded_withNegativeFetchSize() {
    String url = connector.mysqlSetCursorModeIfNeeded("jdbc:mysql://localhost:3306/db", -1);
    assertThat(url).isEqualTo("jdbc:mysql://localhost:3306/db?useCursorFetch=true");
  }

  @Test
  public void testMysqlSetCursorModeIfNeeded_appendsToExistingProperties() {
    String url =
        connector.mysqlSetCursorModeIfNeeded(
            "jdbc:mysql://localhost:3306/db?allowMultiQueries=true", 1000);
    assertThat(url)
        .isEqualTo("jdbc:mysql://localhost:3306/db?allowMultiQueries=true&useCursorFetch=true");
  }
}
