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
package com.google.cloud.teleport.v2.source.mysql.reader.io.jdbc.dialectadapter.mysql;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper.CollationMapper;
import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import com.google.cloud.teleport.v2.source.mysql.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter.MySqlVersion;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.MySQLContainer;

/** Integration tests for MySQL collation mapping against live Testcontainers instances. */
@RunWith(JUnit4.class)
public class MysqlCollationMapperIT {

  @ClassRule
  public static MySQLContainer<?> mysql8Container =
      new MySQLContainer<>("mysql:8.0.36")
          .withDatabaseName("testdb")
          .withUsername("testuser")
          .withPassword("testpass");

  @ClassRule
  public static MySQLContainer<?> mysql57Container =
      new MySQLContainer<>("mysql:5.7.44")
          .withDatabaseName("testdb")
          .withUsername("testuser")
          .withPassword("testpass");

  @Test
  public void testMysql80_Utf8mb40900AiCi() throws SQLException {
    CollationReference collationReference =
        CollationReference.builder()
            .setDbCharacterSet("utf8mb4")
            .setDbCollation("utf8mb4_0900_ai_ci")
            .setPadSpace(false)
            .build();

    try (Connection conn =
        DriverManager.getConnection(
            mysql8Container.getJdbcUrl(),
            mysql8Container.getUsername(),
            mysql8Container.getPassword())) {
      MysqlDialectAdapter adapter = new MysqlDialectAdapter(MySqlVersion.DEFAULT);
      CollationMapper mapper = CollationMapper.fromDB(conn, adapter, collationReference);

      assertThat(mapper.allPositionsIndex().characterToIndex()).isNotEmpty();

      BigInteger cat = mapper.mapString("cat", 3);
      BigInteger mat = mapper.mapString("mat", 3);
      BigInteger avg = cat.add(mat).divide(BigInteger.valueOf(2));
      assertThat(mapper.unMapString(avg)).isEqualTo("ƔAT");

      assertThat(mapper.unMapString(mapper.mapString("cát", 4))).isEqualTo("CAT\t");
    }
  }

  @Test
  public void testMysql80_Utf8mb40900AsCs() throws SQLException {
    CollationReference collationReference =
        CollationReference.builder()
            .setDbCharacterSet("utf8mb4")
            .setDbCollation("utf8mb4_0900_as_cs")
            .setPadSpace(false)
            .build();

    try (Connection conn =
        DriverManager.getConnection(
            mysql8Container.getJdbcUrl(),
            mysql8Container.getUsername(),
            mysql8Container.getPassword())) {
      MysqlDialectAdapter adapter = new MysqlDialectAdapter(MySqlVersion.DEFAULT);
      CollationMapper mapper = CollationMapper.fromDB(conn, adapter, collationReference);

      assertThat(mapper.allPositionsIndex().characterToIndex()).isNotEmpty();
      assertThat(mapper.unMapString(mapper.mapString("cát", 4))).isEqualTo("cát̲");
    }
  }

  @Test
  public void testMysql57_Utf8mb4UnicodeCi() throws SQLException {
    CollationReference collationReference =
        CollationReference.builder()
            .setDbCharacterSet("utf8mb4")
            .setDbCollation("utf8mb4_unicode_ci")
            .setPadSpace(true)
            .build();

    try (Connection conn =
        DriverManager.getConnection(
            mysql57Container.getJdbcUrl(),
            mysql57Container.getUsername(),
            mysql57Container.getPassword())) {
      MysqlDialectAdapter adapter = new MysqlDialectAdapter(MySqlVersion.MYSQL_5_7);
      CollationMapper mapper = CollationMapper.fromDB(conn, adapter, collationReference);

      assertThat(mapper.allPositionsIndex().characterToIndex()).isNotEmpty();
      assertThat(mapper.trailingPositionsPadSpace().characterToIndex()).isNotEmpty();
      assertThat(mapper.unMapString(mapper.mapString("cát", 3))).isEqualTo("CAT");
    }
  }
}
