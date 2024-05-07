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
package com.google.cloud.teleport.v2.options;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link OptionsToConfigBuilder}. */
@RunWith(MockitoJUnitRunner.class)
public class OptionsToConfigBuilderTest {

  @Test
  public void testConfigWithMySqlDefaultsFromOptions() {
    final String testdriverClassName = "org.apache.derby.jdbc.EmbeddedDriver";
    final String testHost = "localHost";
    final String testPort = "3306";
    final String testuser = "user";
    final String testpassword = "password";
    SourceDbToSpannerOptions sourceDbToSpannerOptions =
        PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    sourceDbToSpannerOptions.setSourceHost(testHost);
    sourceDbToSpannerOptions.setSourcePort(testPort);
    sourceDbToSpannerOptions.setJdbcDriverClassName(testdriverClassName);
    sourceDbToSpannerOptions.setSourceConnectionURL("jdbc:derby:memory:testDB;create=true");
    sourceDbToSpannerOptions.setSourceConnectionProperties(
        "maxTotal=160;maxpoolsize=160;maxIdle=160;minIdle=160" + ";wait_timeout=57600");
    sourceDbToSpannerOptions.setFetchSize(50000);
    sourceDbToSpannerOptions.setMaxConnections(150);
    sourceDbToSpannerOptions.setNumPartitions(4000);
    sourceDbToSpannerOptions.setUsername(testuser);
    sourceDbToSpannerOptions.setPassword(testpassword);
    sourceDbToSpannerOptions.setReconnectsEnabled(true);
    sourceDbToSpannerOptions.setReconnectAttempts(10);
    sourceDbToSpannerOptions.setSourceDB("testDB");
    sourceDbToSpannerOptions.setTables("table1,table2");
    sourceDbToSpannerOptions.setPartitionColumns("col1,col2");
    JdbcIOWrapperConfig config =
        OptionsToConfigBuilder.MySql.configWithMySqlDefaultsFromOptions(sourceDbToSpannerOptions);
    assertThat(config.autoReconnect()).isTrue();
    assertThat(config.jdbcDriverClassName()).isEqualTo(testdriverClassName);
    assertThat(config.sourceHost()).isEqualTo(testHost);
    assertThat(config.sourcePort()).isEqualTo(testPort);
    assertThat(
            ImmutableList.of(
                config.tableConfigs().get(0).tableName(), config.tableConfigs().get(1).tableName()))
        .containsExactlyElementsIn(ImmutableList.of("table1", "table2"));
    assertThat(config.dbAuth().getUserName().get()).isEqualTo(testuser);
    assertThat(config.dbAuth().getPassword().get()).isEqualTo(testpassword);
  }
}
