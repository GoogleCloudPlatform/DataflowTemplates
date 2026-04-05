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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.net.URI;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockedStatic;

/** Test class for {@link JdbcUrl}. */
@RunWith(JUnit4.class)
public class JdbcUrlTest {

  @Test
  public void testOf_uriPathNull_returnsNull() {
    try (MockedStatic<URI> mockedUri = mockStatic(URI.class)) {
      URI mockUri = mock(URI.class);
      mockedUri.when(() -> URI.create(anyString())).thenReturn(mockUri);
      when(mockUri.getPath()).thenReturn(null);
      when(mockUri.getScheme()).thenReturn("mysql");

      assertThat(JdbcUrl.of("jdbc:mysql://host/")).isNull();
    }
  }

  @Test
  public void testOf_nullOrEmptyUrl_returnsNull() {
    assertThat(JdbcUrl.of(null)).isNull();
    assertThat(JdbcUrl.of("")).isNull();
  }

  @Test
  public void testOf_noJdbcPrefix_returnsNull() {
    assertThat(JdbcUrl.of("mysql://localhost/db")).isNull();
  }

  @Test
  public void testOf_unsupportedScheme_returnsNull() {
    assertThat(JdbcUrl.of("jdbc:unsupported://localhost/db")).isNull();
  }

  @Test
  public void testOf_postgresql() {
    JdbcUrl jdbcUrl = JdbcUrl.of("jdbc:postgresql://localhost:5432/postgres");
    assertThat(jdbcUrl).isNotNull();
    assertThat(jdbcUrl.getScheme()).isEqualTo("postgresql");
    assertThat(jdbcUrl.getHostAndPort()).isEqualTo("localhost:5432");
    assertThat(jdbcUrl.getDatabase()).isEqualTo("postgres");
  }

  @Test
  public void testOf_mysql() {
    JdbcUrl jdbcUrl = JdbcUrl.of("jdbc:mysql://127.0.0.1:3306/db");
    assertThat(jdbcUrl).isNotNull();
    assertThat(jdbcUrl.getScheme()).isEqualTo("mysql");
    assertThat(jdbcUrl.getHostAndPort()).isEqualTo("127.0.0.1:3306");
    assertThat(jdbcUrl.getDatabase()).isEqualTo("db");
  }

  @Test
  public void testOf_mysqlNoPort() {
    JdbcUrl jdbcUrl = JdbcUrl.of("jdbc:mysql://127.0.0.1/db");
    assertThat(jdbcUrl).isNotNull();
    assertThat(jdbcUrl.getScheme()).isEqualTo("mysql");
    assertThat(jdbcUrl.getHostAndPort()).isNull();
    assertThat(jdbcUrl.getDatabase()).isEqualTo("db");
  }

  @Test
  public void testOf_cloudSql() {
    JdbcUrl jdbcUrl = JdbcUrl.of("jdbc:mysql:///cloud_sql");
    assertThat(jdbcUrl).isNotNull();
    assertThat(jdbcUrl.getScheme()).isEqualTo("mysql");
    assertThat(jdbcUrl.getHostAndPort()).isNull();
    assertThat(jdbcUrl.getDatabase()).isEqualTo("cloud_sql");
  }

  @Test
  public void testOf_oracleThin() {
    JdbcUrl jdbcUrl = JdbcUrl.of("jdbc:oracle:thin:HR/hr@localhost:5221:orcl");
    assertThat(jdbcUrl).isNotNull();
    assertThat(jdbcUrl.getScheme()).isEqualTo("oracle");
    assertThat(jdbcUrl.getHostAndPort()).isEqualTo("localhost:5221");
    assertThat(jdbcUrl.getDatabase()).isEqualTo("orcl");
  }

  @Test
  public void testOf_oracleThin_missingComponents_returnsNull() {
    assertThat(JdbcUrl.of("jdbc:oracle:thin:HR/hr@localhost:5221")).isNull();
  }

  @Test
  public void testOf_oracleThin_noAt_returnsNull() {
    assertThat(JdbcUrl.of("jdbc:oracle:thin:localhost:5221:orcl")).isNull();
  }

  @Test
  public void testOf_oracleThinRac() {
    JdbcUrl jdbcUrl = JdbcUrl.of("jdbc:oracle:thin:@//myhost.example.com:1521/my_service");
    assertThat(jdbcUrl).isNotNull();
    assertThat(jdbcUrl.getScheme()).isEqualTo("oracle");
    assertThat(jdbcUrl.getHostAndPort()).isEqualTo("myhost.example.com:1521");
    assertThat(jdbcUrl.getDatabase()).isEqualTo("my_service");
  }

  @Test
  public void testOf_derby() {
    JdbcUrl jdbcUrl = JdbcUrl.of("jdbc:derby:memory:testDB");
    assertThat(jdbcUrl).isNotNull();
    assertThat(jdbcUrl.getScheme()).isEqualTo("derby");
    assertThat(jdbcUrl.getHostAndPort()).isEqualTo("memory");
    assertThat(jdbcUrl.getDatabase()).isEqualTo("testDB");
  }

  @Test
  public void testOf_derbyWithParams() {
    JdbcUrl jdbcUrl = JdbcUrl.of("jdbc:derby:memory:testDB;create=true");
    assertThat(jdbcUrl).isNotNull();
    assertThat(jdbcUrl.getScheme()).isEqualTo("derby");
    assertThat(jdbcUrl.getHostAndPort()).isEqualTo("memory");
    assertThat(jdbcUrl.getDatabase()).isEqualTo("testDB");
  }

  @Test
  public void testOf_derby_missingComponents_returnsNull() {
    assertThat(JdbcUrl.of("jdbc:derby:memory")).isNull();
  }

  @Test
  public void testOf_unsupportedFormats() {
    assertThat(JdbcUrl.of("jdbc:mysql:")).isNull();
    assertThat(JdbcUrl.of("jdbc:postgresql:")).isNull();
    assertThat(JdbcUrl.of("jdbc:")).isNull();
  }

  @Test
  public void testOf_tooFewParts_returnsNull() {
    // line 59: if (parts.length < 2)
    assertThat(JdbcUrl.of("jdbc")).isNull();
  }

  @Test
  public void testOf_missingPath_returnsNull() {
    // This triggers path == null in URI
    assertThat(JdbcUrl.of("jdbc:mysql:host:port")).isNull();
  }

  @Test
  public void testOf_malformedOracle() {
    assertThat(JdbcUrl.of("jdbc:oracle:thin:HR/hrlocalhost:5221:orcl")).isNull();
  }
}
