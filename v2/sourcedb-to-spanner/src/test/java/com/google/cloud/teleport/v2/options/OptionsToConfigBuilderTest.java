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

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link OptionsToConfigBuilder}. */
@RunWith(MockitoJUnitRunner.class)
public class OptionsToConfigBuilderTest {

  @Test
  public void testconfigWithMySqlDefualtsFromOptions() {
    SourceDbToSpannerOptions sourceDbToSpannerOptions =
        PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    sourceDbToSpannerOptions.setSourceHost("localHost");
    sourceDbToSpannerOptions.setSourcePort("3306");
    sourceDbToSpannerOptions.setJdbcDriverClassName("org.apache.derby.jdbc.EmbeddedDriver");
    sourceDbToSpannerOptions.setSourceConnectionURL("jdbc:derby:memory:testDB;create=true");
    sourceDbToSpannerOptions.setSourceConnectionProperties(
        "maxTotal=160;maxpoolsize=160;maxIdle=160;minIdle=160" + ";wait_timeout=57600");
    sourceDbToSpannerOptions.setFetchSize(50000);
    sourceDbToSpannerOptions.setMaxConnections(150);
    sourceDbToSpannerOptions.setNumPartitions(4000);
    sourceDbToSpannerOptions.setUsername("user");
    sourceDbToSpannerOptions.setPassword("password");
    sourceDbToSpannerOptions.setReconnectsEnabled(true);
    sourceDbToSpannerOptions.setReconnectAttempts(10);
    sourceDbToSpannerOptions.setSourceDB("testDB");
    sourceDbToSpannerOptions.setTables("table1,table2");
    sourceDbToSpannerOptions.setPartitionColumns("col1,col2");
    var config =
        OptionsToConfigBuilder.MySql.configWithMySqlDefualtsFromOptions(sourceDbToSpannerOptions);
    assertThat(config.toString())
        .isEqualTo(
            "JdbcIOWrapperConfig{sourceHost=localHost, sourcePort=3306, sourceSchemaReference=SourceSchemaReference{dbName=testDB, namespace=null}, tableConfigs=[TableConfig{tableName=table1, maxPartitions=4000, maxFetchSize=50000, partitionColumns=[col1]}, TableConfig{tableName=table2, maxPartitions=4000, maxFetchSize=50000, partitionColumns=[col2]}], shardID=Unsupported, dbAuth=LocalCredentialsProvider{userName=user, password=GuardedStringValueProvider{guardedString=org.identityconnectors.common.security.GuardedString@a05005e9}}, jdbcDriverJars=, jdbcDriverClassName=org.apache.derby.jdbc.EmbeddedDriver, schemaMapperType=MYSQL, dialectAdapter=com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter@19382338, valueMappingsProvider=com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.provider.MysqlJdbcValueMappings@66420549, connectionProperties=maxTotal=160;maxpoolsize=160;maxIdle=160;minIdle=160;wait_timeout=57600, autoReconnect=true, reconnectAttempts=10, maxConnections=150, schemaDiscoveryBackOff=FluentBackoff{exponent=1.5, initialBackoff=PT1S, maxBackoff=PT86400000S, maxRetries=2147483647, maxCumulativeBackoff=PT86400000S}}");
  }
}
