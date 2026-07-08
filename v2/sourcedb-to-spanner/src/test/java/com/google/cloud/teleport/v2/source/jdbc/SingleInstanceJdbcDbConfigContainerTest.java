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

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.JdbcIoWrapperConfigGroup;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.common.io.Resources;
import java.nio.file.Paths;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link SingleInstanceJdbcDbConfigContainer}. */
@RunWith(JUnit4.class)
public class SingleInstanceJdbcDbConfigContainerTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testSingleInstanceJdbcDbConfigContainer() {
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
    String sessionFilePath =
        Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
            .toString();
    sourceDbToSpannerOptions.setSessionFilePath(sessionFilePath); // Ensure session file is set

    PCollection<Integer> dummyPCollection = pipeline.apply(Create.of(1));
    pipeline.run();
    SingleInstanceJdbcDbConfigContainer dbConfigContainer =
        new SingleInstanceJdbcDbConfigContainer(sourceDbToSpannerOptions);
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
  }
}
