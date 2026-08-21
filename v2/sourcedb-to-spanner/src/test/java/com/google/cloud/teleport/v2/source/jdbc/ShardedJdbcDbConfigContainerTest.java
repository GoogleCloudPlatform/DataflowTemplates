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
package com.google.cloud.teleport.v2.source.jdbc;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.JdbcIoWrapper;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.JdbcIoWrapperConfigGroup;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.common.collect.ImmutableList;
import java.util.List;
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

/** Test class for {@link ShardedJdbcDbConfigContainer}. */
@RunWith(MockitoJUnitRunner.class)
public class ShardedJdbcDbConfigContainerTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private MockedStatic<JdbcIoWrapper> mockedStaticJdbcIoWrapper;
  @Mock private JdbcIoWrapper mockJdbcIoWrapper;

  @Before
  public void setUp() {
    mockedStaticJdbcIoWrapper = Mockito.mockStatic(JdbcIoWrapper.class);
  }

  @After
  public void tearDown() {
    if (mockedStaticJdbcIoWrapper != null) {
      mockedStaticJdbcIoWrapper.close();
      mockedStaticJdbcIoWrapper = null;
    }
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
    sourceDbToSpannerOptions.setTables("table1,table2");
    mockedStaticJdbcIoWrapper
        .when(() -> JdbcIoWrapper.of(any(JdbcIoWrapperConfigGroup.class)))
        .thenReturn(mockJdbcIoWrapper);

    Shard shard =
        new Shard("shard1", "localhost", "3306", "user", "password", "testDB", null, null, null);
    shard.getDbNameToLogicalShardIdMap().put("testDB", "shard1");

    Shard secondShard =
        new Shard("shard2", "localhost", "3306", "user", "password", "testDB2", null, null, null);
    secondShard.getDbNameToLogicalShardIdMap().put("testDB2", "shard2");

    ShardedJdbcDbConfigContainer dbConfigContainer =
        new ShardedJdbcDbConfigContainer(
            ImmutableList.of(shard, secondShard), SQLDialect.MYSQL, sourceDbToSpannerOptions);

    PCollection<Integer> dummyPCollection = pipeline.apply(Create.of(1));
    pipeline.run();

    JdbcIoWrapperConfigGroup configGroup =
        dbConfigContainer.getJdbcIoWrapperConfigGroup(
            List.of("table1", "table2"), Wait.on(dummyPCollection));
    JdbcIOWrapperConfig config = configGroup.shardConfigs().get(0);

    assertThat(config.jdbcDriverClassName()).isEqualTo(testDriverClassName);
    assertThat(config.sourceDbURL())
        .isEqualTo(
            testUrl
                + "?allowMultiQueries=true&autoReconnect=true&maxReconnects=10&useCursorFetch=true");
    assertThat(config.tables()).containsExactlyElementsIn(new String[] {"table1", "table2"});
    assertThat(config.dbAuth().getUserName().get()).isEqualTo(testUser);
    assertThat(config.dbAuth().getPassword().get()).isEqualTo(testPassword);
    assertThat(config.waitOn()).isNotNull();
    assertThat(
            dbConfigContainer.getIOWrapper(List.of("table1", "table2"), Wait.on(dummyPCollection)))
        .isEqualTo(mockJdbcIoWrapper);
    assertThat(config.shardID()).isEqualTo("shard1");
    assertThat(configGroup.shardConfigs().get(1).shardID()).isEqualTo("shard2");

    assertThat(
            new ShardedJdbcDbConfigContainer(
                    ImmutableList.of(), SQLDialect.MYSQL, sourceDbToSpannerOptions)
                .getJdbcIoWrapperConfigGroup(
                    ImmutableList.of("testTable"), Wait.on(dummyPCollection)))
        .isEqualTo(JdbcIoWrapperConfigGroup.builder().setSourceDbDialect(SQLDialect.MYSQL).build());
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
    sourceDbToSpannerOptions.setTables("table1,table2");

    Shard shard =
        new Shard(
            "shard1",
            "localhost",
            "3306",
            "user",
            "password",
            "testDB",
            "testNameSpace",
            null,
            null);
    shard.getDbNameToLogicalShardIdMap().put("testDB", "shard1");

    ShardedJdbcDbConfigContainer dbConfigContainer =
        new ShardedJdbcDbConfigContainer(
            ImmutableList.of(shard), SQLDialect.POSTGRESQL, sourceDbToSpannerOptions);

    PCollection<Integer> dummyPCollection = pipeline.apply(Create.of(1));
    pipeline.run();

    JdbcIoWrapperConfigGroup configGroup =
        dbConfigContainer.getJdbcIoWrapperConfigGroup(
            List.of("table1", "table2"), Wait.on(dummyPCollection));
    JdbcIOWrapperConfig config = configGroup.shardConfigs().get(0);
    assertThat(config.jdbcDriverClassName()).isEqualTo(testDriverClassName);
    assertThat(config.sourceDbURL()).isEqualTo(testUrl + "?currentSchema=testNameSpace");
    assertThat(config.tables()).containsExactlyElementsIn(new String[] {"table1", "table2"});
    assertThat(config.dbAuth().getUserName().get()).isEqualTo(testUser);
    assertThat(config.dbAuth().getPassword().get()).isEqualTo(testPassword);
    assertThat(config.waitOn()).isNotNull();
  }
}
