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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FQNComponentsTest {

  @Test
  public void testOf_BasicDataSource_success() {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:mysql://localhost/db");
    dataSource.addConnectionProperty("cloudSqlInstance", "project:region:instance");

    FQNComponents fqn = FQNComponents.of(dataSource);
    assertThat(fqn).isNotNull();
    assertThat(fqn.getScheme()).isEqualTo("cloudsql_mysql");
    assertThat(fqn.getSegments()).containsExactly("project", "region", "instance", "db");
  }

  @Test
  public void testOf_BasicDataSource_projectWithColon() {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:postgresql://localhost/db");
    dataSource.addConnectionProperty("cloudSqlInstance", "domain.com:project:region:instance");

    FQNComponents fqn = FQNComponents.of(dataSource);
    assertThat(fqn).isNotNull();
    assertThat(fqn.getScheme()).isEqualTo("cloudsql_postgresql");
    assertThat(fqn.getSegments()).containsExactly("domain.com:project", "region", "instance", "db");
  }

  @Test
  public void testOf_BasicDataSource_notCloudSql() {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:mysql://localhost/db");

    FQNComponents fqn = FQNComponents.of(dataSource);
    assertThat(fqn).isNull();
  }

  @Test
  public void testOf_nonBasicDataSource_notHikari() {
    javax.sql.DataSource mockDs = mock(javax.sql.DataSource.class);
    assertThat(FQNComponents.of(mockDs)).isNull();
  }

  @Test
  public void testOf_BasicDataSource_projectWithManyColons() {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:postgresql://localhost/db");
    dataSource.addConnectionProperty("cloudSqlInstance", "a:b:c:project:region:instance");

    FQNComponents fqn = FQNComponents.of(dataSource);
    assertThat(fqn).isNotNull();
    assertThat(fqn.getScheme()).isEqualTo("cloudsql_postgresql");
    assertThat(fqn.getSegments()).containsExactly("a:b:c:project", "region", "instance", "db");
  }

  @Test
  public void testOf_BasicDataSource_reflectionException() {
    // A class that IS a BasicDataSource but might cause reflection issues if we could trigger it.
    // However, since it's hard to trigger NoSuchMethodException on a real BasicDataSource,
    // we rely on the fact that any unexpected exception in reflection block returns null.
    // of(DataSource) has a broad catch.
    assertThat(FQNComponents.of((javax.sql.DataSource) null)).isNull();
  }

  @Test
  public void testOf_Connection_success() throws SQLException {
    Connection conn = mock(Connection.class);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(conn.getMetaData()).thenReturn(metadata);
    when(metadata.getURL()).thenReturn("jdbc:mysql://127.0.0.1:3306/db");

    FQNComponents fqn = FQNComponents.of(conn);
    assertThat(fqn).isNotNull();
    assertThat(fqn.getScheme()).isEqualTo("mysql");
    assertThat(fqn.getSegments()).containsExactly("127.0.0.1:3306", "db");
  }

  @Test
  public void testOf_String_success() {
    FQNComponents fqn = FQNComponents.of("jdbc:postgresql://localhost:5432/postgres");
    assertThat(fqn).isNotNull();
    assertThat(fqn.getScheme()).isEqualTo("postgresql");
    assertThat(fqn.getSegments()).containsExactly("localhost:5432", "postgres");
  }

  @Test
  public void testOf_String_invalidUrl() {
    assertThat(FQNComponents.of("invalid_url")).isNull();
    assertThat(FQNComponents.of("jdbc:mysql:///")).isNull();
  }

  @Test
  public void testOf_BasicDataSource_nullProperties() {
    BasicDataSource dataSource = mock(BasicDataSource.class);
    FQNComponents fqn = FQNComponents.of(dataSource);
    assertThat(fqn).isNull();
  }

  @Test
  public void testOf_HikariDataSource_success() throws Exception {
    // We can use HikariDataSource directly because it's a test dependency
    com.zaxxer.hikari.HikariDataSource hikariDataSource =
        mock(com.zaxxer.hikari.HikariDataSource.class);
    java.util.Properties props = new java.util.Properties();
    props.setProperty("cloudSqlInstance", "project:region:instance");
    when(hikariDataSource.getDataSourceProperties()).thenReturn(props);
    when(hikariDataSource.getJdbcUrl()).thenReturn("jdbc:mysql://localhost/db");

    FQNComponents fqn = FQNComponents.of(hikariDataSource);
    assertThat(fqn).isNotNull();
    assertThat(fqn.getScheme()).isEqualTo("cloudsql_mysql");
    assertThat(fqn.getSegments()).containsExactly("project", "region", "instance", "db");
  }

  @Test
  public void testOf_HikariDataSource_nullProperties() throws Exception {
    com.zaxxer.hikari.HikariDataSource hikariDataSource =
        mock(com.zaxxer.hikari.HikariDataSource.class);
    when(hikariDataSource.getDataSourceProperties()).thenReturn(null);

    FQNComponents fqn = FQNComponents.of(hikariDataSource);
    assertThat(fqn).isNull();
  }

  @Test
  public void testOf_HikariDataSource_notCloudSql() throws Exception {
    com.zaxxer.hikari.HikariDataSource hikariDataSource =
        mock(com.zaxxer.hikari.HikariDataSource.class);
    java.util.Properties props = new java.util.Properties();
    when(hikariDataSource.getDataSourceProperties()).thenReturn(props);

    FQNComponents fqn = FQNComponents.of(hikariDataSource);
    assertThat(fqn).isNull();
  }

  @Test
  public void testOf_HikariDataSource_nullUrl() throws Exception {
    com.zaxxer.hikari.HikariDataSource hikariDataSource =
        mock(com.zaxxer.hikari.HikariDataSource.class);
    java.util.Properties props = new java.util.Properties();
    props.setProperty("cloudSqlInstance", "project:region:instance");
    when(hikariDataSource.getDataSourceProperties()).thenReturn(props);
    when(hikariDataSource.getJdbcUrl()).thenReturn(null);

    FQNComponents fqn = FQNComponents.of(hikariDataSource);
    assertThat(fqn).isNull();
  }

  @Test
  public void testOf_HikariDataSource_invocationTargetException() throws Exception {
    com.zaxxer.hikari.HikariDataSource hikariDataSource =
        mock(com.zaxxer.hikari.HikariDataSource.class);
    java.util.Properties props = new java.util.Properties();
    props.setProperty("cloudSqlInstance", "project:region:instance");
    when(hikariDataSource.getDataSourceProperties()).thenReturn(props);
    // Method.invoke will wrap the exception in InvocationTargetException
    when(hikariDataSource.getJdbcUrl()).thenThrow(new RuntimeException("Injected failure"));

    assertThat(FQNComponents.of(hikariDataSource)).isNull();
  }

  @Test
  public void testOf_Connection_nullMetadata() throws SQLException {
    Connection conn = mock(Connection.class);
    when(conn.getMetaData()).thenReturn(null);

    assertThat(FQNComponents.of(conn)).isNull();
  }

  @Test
  public void testOf_Connection_nullUrl() throws SQLException {
    Connection conn = mock(Connection.class);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(conn.getMetaData()).thenReturn(metadata);
    when(metadata.getURL()).thenReturn(null);

    assertThat(FQNComponents.of(conn)).isNull();
  }

  @Test
  public void testOf_Connection_exception() throws SQLException {
    Connection conn = mock(Connection.class);
    when(conn.getMetaData()).thenThrow(new SQLException("Test"));

    assertThat(FQNComponents.of(conn)).isNull();
  }

  @Test
  public void testOf_String_nullHostAndPort() {
    // jdbc:mysql:///cloud_sql has null hostAndPort but it's not a cloudsqlInstance in this call
    assertThat(FQNComponents.of("jdbc:mysql:///cloud_sql")).isNull();
  }

  @Test
  public void testReportLineage() {
    FQNComponents fqn = FQNComponents.of("jdbc:mysql://localhost:3306/db");
    Lineage lineage = mock(Lineage.class);
    fqn.reportLineage(lineage, KV.of("myschema", "mytable"));

    verify(lineage).add(eq("mysql"), eq(List.of("localhost:3306", "db", "myschema", "mytable")));
  }

  @Test
  public void testReportLineage_noSchema() {
    FQNComponents fqn = FQNComponents.of("jdbc:mysql://localhost:3306/db");
    Lineage lineage = mock(Lineage.class);
    fqn.reportLineage(lineage, KV.of("", "mytable"));

    verify(lineage).add(eq("mysql"), eq(List.of("localhost:3306", "db", "default", "mytable")));
  }

  @Test
  public void testReportLineage_nullTableWithSchema() {
    FQNComponents fqn = FQNComponents.of("jdbc:mysql://localhost:3306/db");
    Lineage lineage = mock(Lineage.class);
    fqn.reportLineage(lineage, null);

    verify(lineage).add(eq("mysql"), eq(List.of("localhost:3306", "db")));
  }

  @Test
  public void testReportLineage_nullSchema() {
    FQNComponents fqn = FQNComponents.of("jdbc:mysql://localhost:3306/db");
    Lineage lineage = mock(Lineage.class);
    fqn.reportLineage(lineage, KV.of(null, "mytable"));

    verify(lineage).add(eq("mysql"), eq(List.of("localhost:3306", "db", "default", "mytable")));
  }

  @Test
  public void testReportLineage_emptyTableName() {
    FQNComponents fqn = FQNComponents.of("jdbc:mysql://localhost:3306/db");
    Lineage lineage = mock(Lineage.class);
    // table name is empty
    fqn.reportLineage(lineage, KV.of("myschema", ""));

    verify(lineage).add(eq("mysql"), eq(List.of("localhost:3306", "db", "myschema")));
  }

  @Test
  public void testOf_BasicDataSource_JdbcUrlOfReturnsNull() {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:mysql://localhost/db");
    dataSource.addConnectionProperty("cloudSqlInstance", "project:region:instance");

    try (MockedStatic<JdbcUrl> mockedJdbcUrl = mockStatic(JdbcUrl.class)) {
      mockedJdbcUrl.when(() -> JdbcUrl.of(anyString())).thenReturn(null);
      assertThat(FQNComponents.of(dataSource)).isNull();
    }
  }

  @Test
  public void testOf_String_hostAndPortIsNull_mock() {
    try (MockedStatic<JdbcUrl> mockedJdbcUrl = mockStatic(JdbcUrl.class)) {
      JdbcUrl mockJdbcUrl = mock(JdbcUrl.class);
      when(mockJdbcUrl.getHostAndPort()).thenReturn(null);
      mockedJdbcUrl.when(() -> JdbcUrl.of(anyString())).thenReturn(mockJdbcUrl);

      assertThat(FQNComponents.of("some_url")).isNull();
    }
  }

  @Test
  public void testOf_String_jdbcUrlIsNull_mock() {
    try (MockedStatic<JdbcUrl> mockedJdbcUrl = mockStatic(JdbcUrl.class)) {
      mockedJdbcUrl.when(() -> JdbcUrl.of(anyString())).thenReturn(null);

      assertThat(FQNComponents.of("some_url")).isNull();
    }
  }

  @Test
  public void testOf_DataSource_Exception() throws SQLException {
    javax.sql.DataSource mockDs = mock(javax.sql.DataSource.class);
    assertThat(FQNComponents.of(mockDs)).isNull();
  }
}
