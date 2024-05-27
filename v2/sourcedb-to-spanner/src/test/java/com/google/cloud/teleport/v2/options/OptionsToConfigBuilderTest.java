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
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import java.util.ArrayList;
import java.util.List;
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
    final String testUrl = "jdbc:mysql://localhost:3306/testDB";
    final String testuser = "user";
    final String testpassword = "password";
    SourceDbToSpannerOptions sourceDbToSpannerOptions =
        PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    sourceDbToSpannerOptions.setSourceDbURL(testUrl);
    sourceDbToSpannerOptions.setJdbcDriverClassName(testdriverClassName);
    sourceDbToSpannerOptions.setMaxConnections(150);
    sourceDbToSpannerOptions.setNumPartitions(4000);
    sourceDbToSpannerOptions.setUsername(testuser);
    sourceDbToSpannerOptions.setPassword(testpassword);
    sourceDbToSpannerOptions.setTables("table1,table2");
    JdbcIOWrapperConfig config =
        OptionsToConfigBuilder.MySql.configWithMySqlDefaultsFromOptions(
            sourceDbToSpannerOptions, List.of("table1", "table2"));
    assertThat(config.jdbcDriverClassName()).isEqualTo(testdriverClassName);
    assertThat(config.sourceDbURL()).isEqualTo(testUrl);
    assertThat(config.tables()).containsExactlyElementsIn(new String[] {"table1", "table2"});
    assertThat(config.dbAuth().getUserName().get()).isEqualTo(testuser);
    assertThat(config.dbAuth().getPassword().get()).isEqualTo(testpassword);
  }

  @Test
  public void testURIParsingException() {
    final String testUrl = "jd#bc://localhost";
    SourceDbToSpannerOptions sourceDbToSpannerOptions =
        PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    sourceDbToSpannerOptions.setSourceDbURL(testUrl);
    assertThrows(
        RuntimeException.class,
        () ->
            OptionsToConfigBuilder.MySql.configWithMySqlDefaultsFromOptions(
                sourceDbToSpannerOptions, new ArrayList<>()));
  }
}
