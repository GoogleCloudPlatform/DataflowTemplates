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
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link OptionsToConfigBuilder}. */
@RunWith(MockitoJUnitRunner.class)
public class OptionsToConfigBuilderTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testConfigWithMySqlDefaultsFromOptions() {
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
    PCollection<Integer> dummyPCollection = pipeline.apply(Create.of(1));
    pipeline.run();
    JdbcIOWrapperConfig config =
        OptionsToConfigBuilder.getJdbcIOWrapperConfigWithDefaults(
            sourceDbToSpannerOptions, List.of("table1", "table2"), null, Wait.on(dummyPCollection));
    assertThat(config.jdbcDriverClassName()).isEqualTo(testDriverClassName);
    assertThat(config.sourceDbURL())
        .isEqualTo(testUrl + "?allowMultiQueries=true&autoReconnect=true&maxReconnects=10");
    assertThat(config.tables()).containsExactlyElementsIn(new String[] {"table1", "table2"});
    assertThat(config.dbAuth().getUserName().get()).isEqualTo(testUser);
    assertThat(config.dbAuth().getPassword().get()).isEqualTo(testPassword);
    assertThat(config.waitOn()).isNotNull();
    assertThat(config.maxFetchSize()).isNull();
    sourceDbToSpannerOptions.setFetchSize(42);
    assertThat(
            OptionsToConfigBuilder.getJdbcIOWrapperConfigWithDefaults(
                    sourceDbToSpannerOptions,
                    List.of("table1", "table2"),
                    null,
                    Wait.on(dummyPCollection))
                .maxFetchSize())
        .isEqualTo(42);
  }

  @Test
  public void testConfigWithMySqlUrlFromOptions() {
    PCollection<Integer> dummyPCollection = pipeline.apply(Create.of(1));
    pipeline.run();
    JdbcIOWrapperConfig configWithConnectionProperties =
        OptionsToConfigBuilder.getJdbcIOWrapperConfig(
            SQLDialect.MYSQL,
            List.of("table1", "table2"),
            null,
            "myhost",
            "testParam=testValue",
            3306,
            "myuser",
            "mypassword",
            "mydb",
            null,
            null,
            "com.mysql.jdbc.Driver",
            "mysql-jar",
            10,
            0,
            Wait.on(dummyPCollection),
            null,
            0L,
            null,
            null);

    JdbcIOWrapperConfig configWithoutConnectionProperties =
        OptionsToConfigBuilder.getJdbcIOWrapperConfig(
            SQLDialect.MYSQL,
            List.of("table1", "table2"),
            null,
            "myhost",
            null,
            3306,
            "myuser",
            "mypassword",
            "mydb",
            null,
            null,
            "com.mysql.jdbc.Driver",
            "mysql-jar",
            10,
            0,
            Wait.on(dummyPCollection),
            null,
            0L,
            null,
            null);

    assertThat(configWithConnectionProperties.sourceDbURL())
        .isEqualTo(
            "jdbc:mysql://myhost:3306/mydb?testParam=testValue&allowMultiQueries=true&autoReconnect=true&maxReconnects=10");
    assertThat(configWithoutConnectionProperties.sourceDbURL())
        .isEqualTo(
            "jdbc:mysql://myhost:3306/mydb?allowMultiQueries=true&autoReconnect=true&maxReconnects=10");
  }

  @Test
  public void testConfigWithPostgreSQLDefaultsFromOptions() {
    final String testDriverClassName = "org.apache.derby.jdbc.EmbeddedDriver";
    final String testUrl = "jdbc:postgresql://localhost:5432/testDB";
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
    sourceDbToSpannerOptions.setTables("table1,table2,table3");
    PCollection<Integer> dummyPCollection = pipeline.apply(Create.of(1));
    pipeline.run();
    JdbcIOWrapperConfig config =
        OptionsToConfigBuilder.getJdbcIOWrapperConfigWithDefaults(
            sourceDbToSpannerOptions,
            List.of("table1", "table2", "table3"),
            null,
            Wait.on(dummyPCollection));
    assertThat(config.jdbcDriverClassName()).isEqualTo(testDriverClassName);
    assertThat(config.sourceDbURL()).isEqualTo(testUrl + "?currentSchema=public");
    assertThat(config.tables())
        .containsExactlyElementsIn(new String[] {"table1", "table2", "table3"});
    assertThat(config.dbAuth().getUserName().get()).isEqualTo(testUser);
    assertThat(config.dbAuth().getPassword().get()).isEqualTo(testPassword);
    assertThat(config.waitOn()).isNotNull();
  }

  @Test
  public void testConfigWithPostgreSqlUrlFromOptions() {
    PCollection<Integer> dummyPCollection = pipeline.apply(Create.of(1));
    pipeline.run();
    JdbcIOWrapperConfig configWithConnectionParameters =
        OptionsToConfigBuilder.getJdbcIOWrapperConfig(
            SQLDialect.POSTGRESQL,
            List.of("table1", "table2"),
            null,
            "myhost",
            "testParam=testValue",
            5432,
            "myuser",
            "mypassword",
            "mydb",
            null,
            null,
            "com.mysql.jdbc.Driver",
            "mysql-jar",
            10,
            0,
            Wait.on(dummyPCollection),
            null,
            0L,
            null,
            null);
    JdbcIOWrapperConfig configWithoutConnectionParameters =
        OptionsToConfigBuilder.getJdbcIOWrapperConfig(
            SQLDialect.POSTGRESQL,
            List.of("table1", "table2"),
            null,
            "myhost",
            "",
            5432,
            "myuser",
            "mypassword",
            "mydb",
            null,
            null,
            "com.mysql.jdbc.Driver",
            "mysql-jar",
            10,
            0,
            Wait.on(dummyPCollection),
            null,
            -1L,
            null,
            null);
    assertThat(configWithoutConnectionParameters.sourceDbURL())
        .isEqualTo("jdbc:postgresql://myhost:5432/mydb?currentSchema=public");
    assertThat(configWithConnectionParameters.sourceDbURL())
        .isEqualTo("jdbc:postgresql://myhost:5432/mydb?currentSchema=public&testParam=testValue");
    assertThat(configWithConnectionParameters.splitStageCountHint()).isEqualTo(0L);
    assertThat(configWithoutConnectionParameters.splitStageCountHint()).isEqualTo(-1L);
  }

  @Test
  public void testConfigWithPostgreSqlUrlWithNamespace() {
    PCollection<Integer> dummyPCollection = pipeline.apply(Create.of(1));
    pipeline.run();
    JdbcIOWrapperConfig configWithNamespace =
        OptionsToConfigBuilder.getJdbcIOWrapperConfig(
            SQLDialect.POSTGRESQL,
            List.of("table1", "table2"),
            null,
            "myhost",
            "",
            5432,
            "myuser",
            "mypassword",
            "mydb",
            "mynamespace",
            null,
            "com.mysql.jdbc.Driver",
            "mysql-jar",
            10,
            0,
            Wait.on(dummyPCollection),
            null,
            0L,
            null,
            null);
    assertThat(configWithNamespace.sourceDbURL())
        .isEqualTo("jdbc:postgresql://myhost:5432/mydb?currentSchema=mynamespace");
  }

  @Test
  public void testURIParsingException() {
    final String testUrl = "jd#bc://localhost";
    SourceDbToSpannerOptions sourceDbToSpannerOptions =
        PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    sourceDbToSpannerOptions.setSourceDbDialect(SQLDialect.MYSQL.name());
    sourceDbToSpannerOptions.setSourceConfigURL(testUrl);
    assertThrows(
        RuntimeException.class,
        () ->
            OptionsToConfigBuilder.getJdbcIOWrapperConfigWithDefaults(
                sourceDbToSpannerOptions, new ArrayList<>(), null, null));
  }

  @Test
  public void testaddParamToJdbcUrl() throws URISyntaxException {
    // No Parameters initially.
    assertThat(
            OptionsToConfigBuilder.addParamToJdbcUrl(
                "jdbc:mysql://localhost:3306/testDB", "allowMultiQueries", "true"))
        .isEqualTo("jdbc:mysql://localhost:3306/testDB?allowMultiQueries=true");
    assertThat(
            OptionsToConfigBuilder.addParamToJdbcUrl(
                "jdbc:mysql://localhost:3306/testDB?", "allowMultiQueries", "true"))
        .isEqualTo("jdbc:mysql://localhost:3306/testDB?allowMultiQueries=true");
    // Other Parameters present.
    assertThat(
            OptionsToConfigBuilder.addParamToJdbcUrl(
                "jdbc:mysql://localhost:3306/testDB?useSSL=true&autoReconnect=true",
                "allowMultiQueries",
                "true"))
        .isEqualTo(
            "jdbc:mysql://localhost:3306/testDB?useSSL=true&autoReconnect=true&allowMultiQueries=true");
    // Parameter present with same value.
    assertThat(
            OptionsToConfigBuilder.addParamToJdbcUrl(
                "jdbc:mysql://localhost:3306/testDB?useSSL=true&autoReconnect=true&allowMultiQueries=true",
                "allowMultiQueries",
                "true"))
        .isEqualTo(
            "jdbc:mysql://localhost:3306/testDB?useSSL=true&autoReconnect=true&allowMultiQueries=true");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            OptionsToConfigBuilder.addParamToJdbcUrl(
                "jdbc:mysql://localhost:3306/testDB?useSSL=true&autoReconnect=true&allowMultiQueries=false",
                "allowMultiQueries",
                "true"));
  }

  @Test
  public void testMySqlSetCursorModeIfNeeded() {
    assertThat(
            OptionsToConfigBuilder.mysqlSetCursorModeIfNeeded(
                SQLDialect.MYSQL, "jdbc:mysql://localhost:3306/testDB?useSSL=true", 42))
        .isEqualTo("jdbc:mysql://localhost:3306/testDB?useSSL=true&useCursorFetch=true");
    assertThat(
            OptionsToConfigBuilder.mysqlSetCursorModeIfNeeded(
                SQLDialect.MYSQL, "jdbc:mysql://localhost:3306/testDB?useSSL=true", null))
        .isEqualTo("jdbc:mysql://localhost:3306/testDB?useSSL=true");
    assertThat(
            OptionsToConfigBuilder.mysqlSetCursorModeIfNeeded(
                SQLDialect.POSTGRESQL, "jdbc:mysql://localhost:3306/testDB?useSSL=true", 42))
        .isEqualTo("jdbc:mysql://localhost:3306/testDB?useSSL=true");
  }

  @Test
  public void testWorkerZoneExtractionException() {
    SourceDbToSpannerOptions mockOptions = Mockito.mock(SourceDbToSpannerOptions.class);
    Mockito.when(mockOptions.getSourceDbDialect()).thenReturn(SQLDialect.MYSQL.name());
    Mockito.when(mockOptions.getSourceConfigURL()).thenReturn("jdbc:mysql://localhost:3306/testDB");
    Mockito.when(mockOptions.getJdbcDriverClassName()).thenReturn("com.mysql.jdbc.Driver");
    Mockito.when(mockOptions.getUsername()).thenReturn("user");
    Mockito.when(mockOptions.getPassword()).thenReturn("password");
    Mockito.when(mockOptions.getJdbcDriverJars()).thenReturn("driver.jar");
    Mockito.when(mockOptions.getProjectId()).thenReturn("project-id");
    Mockito.when(mockOptions.getJdbcDriverClassName()).thenReturn("com.mysql.jdbc.Driver");
    Mockito.when(mockOptions.as(DataflowPipelineWorkerPoolOptions.class))
        .thenThrow(new RuntimeException("Test Exception"));

    JdbcIOWrapperConfig config =
        OptionsToConfigBuilder.getJdbcIOWrapperConfigWithDefaults(
            mockOptions, List.of("table1"), null, null);

    assertThat(config.workerZone()).isNull();
  }

  @Test
  public void testExtractWorkerZone() {
    DataflowPipelineWorkerPoolOptions mockOptions =
        Mockito.mock(DataflowPipelineWorkerPoolOptions.class);
    Mockito.when(mockOptions.getWorkerZone()).thenReturn("us-central1-a");
    Mockito.when(mockOptions.as(DataflowPipelineWorkerPoolOptions.class)).thenReturn(mockOptions);

    String workerZone = OptionsToConfigBuilder.extractWorkerZone(mockOptions);
    assertThat(workerZone).isEqualTo("us-central1-a");
  }

  @Test
  public void testExtractWorkerZoneException() {
    DataflowPipelineWorkerPoolOptions mockOptions =
        Mockito.mock(DataflowPipelineWorkerPoolOptions.class);
    Mockito.when(mockOptions.as(DataflowPipelineWorkerPoolOptions.class))
        .thenThrow(new RuntimeException("Test Exception"));

    String workerZone = OptionsToConfigBuilder.extractWorkerZone(mockOptions);
    assertThat(workerZone).isNull();
  }
}
