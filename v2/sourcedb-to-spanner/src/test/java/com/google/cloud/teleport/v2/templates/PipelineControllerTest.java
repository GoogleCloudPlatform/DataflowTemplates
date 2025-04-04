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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.JdbcIoWrapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.PipelineController.ShardedJdbcDbConfigContainer;
import com.google.cloud.teleport.v2.templates.PipelineController.SingleInstanceJdbcDbConfigContainer;
import com.google.common.io.Resources;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.quality.Strictness;

@RunWith(MockitoJUnitRunner.class)
public class PipelineControllerTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private Ddl spannerDdl;

  private Ddl shardedDdl;

  private MockedStatic<JdbcIoWrapper> mockedStaticJdbcIoWrapper;
  @Mock private JdbcIoWrapper mockJdbcIoWrapper;

  @Before
  public void setup() {
    mockedStaticJdbcIoWrapper = Mockito.mockStatic(JdbcIoWrapper.class);
    spannerDdl =
        Ddl.builder()
            .createTable("new_cart")
            .column("new_quantity")
            .int64()
            .notNull()
            .endColumn()
            .column("new_user_id")
            .string()
            .size(10)
            .endColumn()
            .primaryKey()
            .asc("new_user_id")
            .asc("new_quantity")
            .end()
            .endTable()
            .createTable("new_people")
            .column("synth_id")
            .int64()
            .notNull()
            .endColumn()
            .column("new_name")
            .string()
            .size(10)
            .endColumn()
            .primaryKey()
            .asc("synth_id")
            .end()
            .endTable()
            .build();

    shardedDdl =
        Ddl.builder()
            .createTable("new_cart")
            .column("new_quantity")
            .int64()
            .notNull()
            .endColumn()
            .column("new_product_id")
            .string()
            .size(20)
            .endColumn()
            .column("new_user_id")
            .string()
            .size(20)
            .endColumn()
            .primaryKey()
            .asc("new_user_id")
            .asc("new_product_id")
            .end()
            .endTable()
            .createTable("new_people")
            .column("migration_shard_id")
            .string()
            .size(20)
            .endColumn()
            .column("new_name")
            .string()
            .size(20)
            .endColumn()
            .primaryKey()
            .asc("migration_shard_id")
            .asc("new_name")
            .end()
            .endTable()
            .build();
  }

  @Test
  public void createIdentitySchemaMapper() {
    SourceDbToSpannerOptions mockOptions = createOptionsHelper("", "");
    ISchemaMapper schemaMapper = PipelineController.getSchemaMapper(mockOptions, spannerDdl);
    assertTrue(schemaMapper instanceof IdentityMapper);
  }

  @Test
  public void createSessionSchemaMapper() {
    SourceDbToSpannerOptions mockOptions =
        createOptionsHelper(
            Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
                .toString(),
            null);
    ISchemaMapper schemaMapper = PipelineController.getSchemaMapper(mockOptions, spannerDdl);
    assertTrue(schemaMapper instanceof SessionBasedMapper);
  }

  @Test(expected = Exception.class)
  public void createInvalidSchemaMapper_withException() {
    SourceDbToSpannerOptions mockOptions = createOptionsHelper("invalid-file", "");
    PipelineController.getSchemaMapper(mockOptions, spannerDdl);
  }

  @Test
  public void tableToShardIdColumnNonSharded() {
    String sessionFilePath =
        Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
            .toString();
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFilePath, spannerDdl);
    Map<String, String> mapAllTables =
        PipelineController.getSrcTableToShardIdColumnMap(
            schemaMapper, "", List.of("new_cart", "new_people"));
    assertThat(mapAllTables.isEmpty());
  }

  @Test
  public void tableToShardIdColumnSharded() {
    String shardedSessionFilePath =
        Paths.get(Resources.getResource("session-file-sharded.json").getPath()).toString();
    ISchemaMapper schemaMapper = new SessionBasedMapper(shardedSessionFilePath, shardedDdl);
    Map<String, String> mapAllTables =
        PipelineController.getSrcTableToShardIdColumnMap(
            schemaMapper, "", List.of("new_cart", "new_people"));
    assertEquals(1, mapAllTables.size());
    assertEquals("migration_shard_id", mapAllTables.get("people"));
  }

  @Test(expected = NoSuchElementException.class)
  public void tableToShardIdColumnInvalidTable() {
    String sessionFilePath =
        Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
            .toString();
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFilePath, spannerDdl);
    PipelineController.getSrcTableToShardIdColumnMap(
        schemaMapper, "", List.of("cart")); // Only accepts spanner table names
  }

  private SourceDbToSpannerOptions createOptionsHelper(String sessionFile, String tables) {
    /*
     * This mock is used in exception paths where all the stubs don't get used.
     */
    SourceDbToSpannerOptions mockOptions =
        mock(
            SourceDbToSpannerOptions.class,
            Mockito.withSettings().serializable().strictness(Strictness.LENIENT));
    when(mockOptions.getSessionFilePath()).thenReturn(sessionFile);
    when(mockOptions.getTables()).thenReturn(tables);
    return mockOptions;
  }

  @Test
  public void singleDbConfigContainerWithUrlTest() {
    // Most of this is copied from OptionsToConfigBuilderTest.
    // Check if it is possible to re-use
    final String testDriverClassName = "org.apache.derby.jdbc.EmbeddedDriver";
    final String testUrl = "jdbc:mysql://localhost:3306/testDB";
    final String testUser = "user";
    final String testPassword = "password";
    SourceDbToSpannerOptions sourceDbToSpannerOptions =
        PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    sourceDbToSpannerOptions.setSourceDbDialect(SQLDialect.MYSQL.name());
    sourceDbToSpannerOptions.setSourceConfigURL(testUrl);
    sourceDbToSpannerOptions.setJdbcDriverClassName(testDriverClassName);
    sourceDbToSpannerOptions.setMaxConnections(150);
    sourceDbToSpannerOptions.setNumPartitions(4000);
    sourceDbToSpannerOptions.setUsername(testUser);
    sourceDbToSpannerOptions.setPassword(testPassword);
    sourceDbToSpannerOptions.setTables("table1,table2");
    String sessionFilePath =
        Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
            .toString();
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFilePath, spannerDdl);
    PCollection<Integer> dummyPCollection = pipeline.apply(Create.of(1));
    pipeline.run();
    SingleInstanceJdbcDbConfigContainer dbConfigContainer =
        new SingleInstanceJdbcDbConfigContainer(sourceDbToSpannerOptions);
    JdbcIOWrapperConfig config =
        dbConfigContainer.getJDBCIOWrapperConfig(
            List.of("table1", "table2"), Wait.on(dummyPCollection));
    assertThat(config.jdbcDriverClassName()).isEqualTo(testDriverClassName);
    assertThat(config.sourceDbURL())
        .isEqualTo(testUrl + "?allowMultiQueries=true&autoReconnect=true&maxReconnects=10");
    assertThat(config.tables()).containsExactlyElementsIn(new String[] {"table1", "table2"});
    assertThat(config.dbAuth().getUserName().get()).isEqualTo(testUser);
    assertThat(config.dbAuth().getPassword().get()).isEqualTo(testPassword);
    assertThat(config.waitOn()).isNotNull();
    assertEquals(null, dbConfigContainer.getShardId());
    assertThat(dbConfigContainer.getSrcTableToShardIdColumnMap(schemaMapper, List.of("new_cart")))
        .isEqualTo(new HashMap<>());
  }

  @Test
  public void shardedDbConfigContainerMySqlTest() {
    final String testDriverClassName = "org.apache.derby.jdbc.EmbeddedDriver";
    final String testUrl = "jdbc:mysql://localhost:3306/testDB";
    final String testUser = "user";
    final String testPassword = "password";
    SourceDbToSpannerOptions sourceDbToSpannerOptions =
        PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    sourceDbToSpannerOptions.setSourceDbDialect(SQLDialect.MYSQL.name());
    sourceDbToSpannerOptions.setSourceConfigURL(testUrl);
    sourceDbToSpannerOptions.setJdbcDriverClassName(testDriverClassName);
    sourceDbToSpannerOptions.setMaxConnections(150);
    sourceDbToSpannerOptions.setNumPartitions(4000);
    sourceDbToSpannerOptions.setUsername(testUser);
    sourceDbToSpannerOptions.setPassword(testPassword);
    sourceDbToSpannerOptions.setTables("table1,table2");
    mockedStaticJdbcIoWrapper.when(() -> JdbcIoWrapper.of(any())).thenReturn(mockJdbcIoWrapper);

    Shard shard =
        new Shard("shard1", "localhost", "3306", "user", "password", null, null, null, null);

    ShardedJdbcDbConfigContainer dbConfigContainer =
        new ShardedJdbcDbConfigContainer(
            shard, SQLDialect.MYSQL, null, "shard1", "testDB", sourceDbToSpannerOptions);

    PCollection<Integer> dummyPCollection = pipeline.apply(Create.of(1));
    pipeline.run();

    JdbcIOWrapperConfig config =
        dbConfigContainer.getJDBCIOWrapperConfig(
            List.of("table1", "table2"), Wait.on(dummyPCollection));

    assertThat(config.jdbcDriverClassName()).isEqualTo(testDriverClassName);
    assertThat(config.sourceDbURL())
        .isEqualTo(testUrl + "?allowMultiQueries=true&autoReconnect=true&maxReconnects=10");
    assertThat(config.tables()).containsExactlyElementsIn(new String[] {"table1", "table2"});
    assertThat(config.dbAuth().getUserName().get()).isEqualTo(testUser);
    assertThat(config.dbAuth().getPassword().get()).isEqualTo(testPassword);
    assertThat(config.waitOn()).isNotNull();
    assertEquals("shard1", dbConfigContainer.getShardId());
    assertThat(
            dbConfigContainer.getIOWrapper(List.of("table1", "table2"), Wait.on(dummyPCollection)))
        .isEqualTo(mockJdbcIoWrapper);
  }

  @Test
  public void shardedDbConfigContainerPGTest() {
    final String testDriverClassName = "org.apache.derby.jdbc.EmbeddedDriver";
    final String testUrl = "jdbc:postgresql://localhost:3306/testDB";
    final String testUser = "user";
    final String testPassword = "password";
    SourceDbToSpannerOptions sourceDbToSpannerOptions =
        PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    sourceDbToSpannerOptions.setSourceDbDialect(SQLDialect.POSTGRESQL.name());
    sourceDbToSpannerOptions.setSourceConfigURL(testUrl);
    sourceDbToSpannerOptions.setJdbcDriverClassName(testDriverClassName);
    sourceDbToSpannerOptions.setMaxConnections(150);
    sourceDbToSpannerOptions.setNumPartitions(4000);
    sourceDbToSpannerOptions.setUsername(testUser);
    sourceDbToSpannerOptions.setPassword(testPassword);
    sourceDbToSpannerOptions.setTables("table1,table2");

    Shard shard =
        new Shard("shard1", "localhost", "3306", "user", "password", null, null, null, null);

    ShardedJdbcDbConfigContainer dbConfigContainer =
        new ShardedJdbcDbConfigContainer(
            shard,
            SQLDialect.POSTGRESQL,
            "testNameSpace",
            "shard1",
            "testDB",
            sourceDbToSpannerOptions);

    PCollection<Integer> dummyPCollection = pipeline.apply(Create.of(1));
    pipeline.run();

    JdbcIOWrapperConfig config =
        dbConfigContainer.getJDBCIOWrapperConfig(
            List.of("table1", "table2"), Wait.on(dummyPCollection));
    assertThat(config.jdbcDriverClassName()).isEqualTo(testDriverClassName);
    assertThat(config.sourceDbURL()).isEqualTo(testUrl + "?currentSchema=testNameSpace");
    assertThat(config.tables()).containsExactlyElementsIn(new String[] {"table1", "table2"});
    assertThat(config.dbAuth().getUserName().get()).isEqualTo(testUser);
    assertThat(config.dbAuth().getPassword().get()).isEqualTo(testPassword);
    assertThat(config.waitOn()).isNotNull();
    assertEquals("shard1", dbConfigContainer.getShardId());
    assertEquals("testNameSpace", dbConfigContainer.getNamespace());
  }

  @After
  public void cleanup() {
    mockedStaticJdbcIoWrapper.close();
    mockedStaticJdbcIoWrapper = null;
  }
}
