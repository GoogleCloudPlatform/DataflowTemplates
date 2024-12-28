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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.source.reader.auth.dbauth.LocalCredentialsProvider;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.defaults.MySqlConfigDefaults;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link JdbcDataSource}. */
@RunWith(MockitoJUnitRunner.class)
public class JdbcDataSourceTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Mock DialectAdapter mockDialectAdapter;

  @Test
  public void testJdbcDataSourceBasic() throws IOException, ClassNotFoundException {

    JdbcSchemaReference testSourceSchemaReference =
        JdbcSchemaReference.builder().setDbName("testDB").build();

    JdbcIOWrapperConfig jdbcIOWrapperConfig =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:derby://myhost/memory:TestingDB;create=true")
            .setSourceSchemaReference(testSourceSchemaReference)
            .setShardID("test")
            .setDbAuth(
                LocalCredentialsProvider.builder()
                    .setUserName("testUser")
                    .setPassword("testPassword")
                    .build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("org.apache.derby.jdbc.EmbeddedDriver")
            .setDialectAdapter(mockDialectAdapter)
            .build();
    JdbcDataSource testJdbcDataSource = new JdbcDataSource(jdbcIOWrapperConfig);

    // Check that testDataSource is initialized correctly.
    assertThat(testJdbcDataSource.getConnectionInitSqls())
        .isEqualTo(jdbcIOWrapperConfig.sqlInitSeq());
    assertThat(testJdbcDataSource.getDriverClassName())
        .isEqualTo(jdbcIOWrapperConfig.jdbcDriverClassName());
    assertThat(testJdbcDataSource.getMaxTotal()).isEqualTo(jdbcIOWrapperConfig.maxConnections());
    assertThat(testJdbcDataSource.getRemoveAbandonedTimeout())
        .isEqualTo(jdbcIOWrapperConfig.removeAbandonedTimeout());
    assertThat(testJdbcDataSource.getTestOnBorrow()).isEqualTo(jdbcIOWrapperConfig.testOnBorrow());
    assertThat(testJdbcDataSource.getTestOnCreate()).isEqualTo(jdbcIOWrapperConfig.testOnCreate());
    assertThat(testJdbcDataSource.getTestWhileIdle())
        .isEqualTo(jdbcIOWrapperConfig.testWhileIdle());
    assertThat(testJdbcDataSource.getMinEvictableIdleTimeMillis())
        .isEqualTo(jdbcIOWrapperConfig.minEvictableIdleTimeMillis());
    assertThat(testJdbcDataSource.getValidationQuery())
        .isEqualTo(jdbcIOWrapperConfig.validationQuery());
    assertThat(testJdbcDataSource.toString())
        .isEqualTo(
            "JdbcDataSource: {\"sourceDbURL\":\"jdbc:derby://myhost/memory:TestingDB;create=true\", \"initSql\":\"[SET TIME_ZONE = '+00:00', SET SESSION NET_WRITE_TIMEOUT=1200, SET SESSION NET_READ_TIMEOUT=1200, "
                + MySqlConfigDefaults.ENABLE_ANSI_QUOTES_INIT_SEQ
                + "]\", \"maxConnections\",\"160\" }");
  }

  @Test
  public void testJdbcDataSourceSerDe() throws IOException, ClassNotFoundException {

    JdbcSchemaReference testSourceSchemaReference =
        JdbcSchemaReference.builder().setDbName("testDB").build();

    JdbcIOWrapperConfig jdbcIOWrapperConfig =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:derby://myhost/memory:TestingDB;create=true")
            .setSourceSchemaReference(testSourceSchemaReference)
            .setShardID("test")
            .setSqlInitSeq(ImmutableList.of())
            .setDbAuth(
                LocalCredentialsProvider.builder()
                    .setUserName("testUser")
                    .setPassword("testPassword")
                    .build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("org.apache.derby.jdbc.EmbeddedDriver")
            .setDialectAdapter(mockDialectAdapter)
            .build();
    JdbcDataSource testJdbcDataSource = new JdbcDataSource(jdbcIOWrapperConfig);

    // Serialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(testJdbcDataSource);
    oos.close();

    // Deserialize
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    JdbcDataSource deserializedDataSource = (JdbcDataSource) ois.readObject();
    ois.close();

    // Check that the deserialized object initializes the datasource correctly.
    assertThat(deserializedDataSource.getConnectionInitSqls())
        .isEqualTo(testJdbcDataSource.getConnectionInitSqls());
    assertThat(deserializedDataSource.getDriverClassName())
        .isEqualTo(testJdbcDataSource.getDriverClassName());
    assertThat(deserializedDataSource.getMaxTotal()).isEqualTo(testJdbcDataSource.getMaxTotal());
    assertThat(deserializedDataSource.getMaxIdle()).isEqualTo(testJdbcDataSource.getMaxIdle());
    assertThat(deserializedDataSource.getTestOnBorrow())
        .isEqualTo(testJdbcDataSource.getTestOnBorrow());
    assertThat(deserializedDataSource.getTestOnCreate())
        .isEqualTo(testJdbcDataSource.getTestOnCreate());
    assertThat(deserializedDataSource.getTestWhileIdle())
        .isEqualTo(testJdbcDataSource.getTestWhileIdle());
    assertThat(deserializedDataSource.getRemoveAbandonedTimeout())
        .isEqualTo(testJdbcDataSource.getRemoveAbandonedTimeout());
    assertThat(deserializedDataSource.getMinEvictableIdleTimeMillis())
        .isEqualTo(testJdbcDataSource.getMinEvictableIdleTimeMillis());
    assertThat(deserializedDataSource.getValidationQuery())
        .isEqualTo(testJdbcDataSource.getValidationQuery());
  }
}
