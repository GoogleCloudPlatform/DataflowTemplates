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
package com.google.cloud.teleport.v2.source.spanner;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.model.SinkConfig;
import com.google.cloud.teleport.v2.templates.sink.DataWriter;
import java.io.File;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link SpannerSourceConnector}. */
@RunWith(JUnit4.class)
public class SpannerSourceConnectorTest {

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final SpannerSourceConnector connector = new SpannerSourceConnector();

  @After
  public void tearDown() {
    SpannerSourceConnector.setDialectProvider(null);
  }

  @Test
  public void testGetSourceTypeAndSinkType() {
    assertThat(connector.getSourceType()).isEqualTo("SPANNER");
    assertThat(connector.getSinkType()).isEqualTo(SinkType.SPANNER);
  }

  @Test
  public void testParseConfig_nullOrEmptyThrows() {
    assertThrows(IllegalArgumentException.class, () -> connector.parseConfig(null));
    assertThrows(IllegalArgumentException.class, () -> connector.parseConfig(""));
  }

  @Test
  public void testParseConfig_success() throws Exception {
    String spannerJson =
        "{\n"
            + "  \"projectId\": \"sp-project\",\n"
            + "  \"instanceId\": \"sp-instance\",\n"
            + "  \"databaseId\": \"sp-database\"\n"
            + "}";
    File file = tempFolder.newFile("spanner_config.json");
    FileUtils.writeStringToFile(file, spannerJson, StandardCharsets.UTF_8);

    SpannerSourceConnector.setDialectProvider(
        (projectId, instanceId, databaseId) -> Dialect.GOOGLE_STANDARD_SQL);

    SinkConfig config = connector.parseConfig(file.getAbsolutePath());
    assertThat(config).isInstanceOf(SpannerSinkConfig.class);
    SpannerSinkConfig spannerConfig = (SpannerSinkConfig) config;
    assertThat(spannerConfig.getProjectId()).isEqualTo("sp-project");
    assertThat(spannerConfig.getInstanceId()).isEqualTo("sp-instance");
    assertThat(spannerConfig.getDatabaseId()).isEqualTo("sp-database");
    assertThat(spannerConfig.getDialect()).isEqualTo(Dialect.GOOGLE_STANDARD_SQL);
  }

  @Test
  public void testGetSchemaFetcherAndTypeMapper() {
    assertThat(connector.getSchemaFetcher()).isInstanceOf(SpannerSchemaFetcher.class);
    assertThat(connector.getTypeMapper()).isInstanceOf(SpannerTypeMapper.class);
  }

  @Test
  public void testCreateDataWriter() {
    SpannerSinkConfig config =
        new SpannerSinkConfig("p", "i", "d", Dialect.GOOGLE_STANDARD_SQL);
    DataWriter writer = connector.createDataWriter(config);
    assertThat(writer).isInstanceOf(SpannerDataWriter.class);
  }

  @Test
  public void testCreateDataWriter_invalidConfigThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> connector.createDataWriter(new SinkConfig() {}));
  }

  @Test
  public void testResolveKeyParallelism() {
    SpannerSinkConfig config =
        new SpannerSinkConfig("p", "i", "d", Dialect.GOOGLE_STANDARD_SQL);
    assertThat(connector.resolveKeyParallelism(config)).isEqualTo(100000);
    assertThat(connector.resolveKeyParallelism(null)).isEqualTo(100000);
  }

  @Test
  public void testGetLogicalShardIds() {
    SpannerSinkConfig config =
        new SpannerSinkConfig("p", "i", "d", Dialect.GOOGLE_STANDARD_SQL);
    assertThat(connector.getLogicalShardIds(config)).isEmpty();
    assertThat(connector.getLogicalShardIds(null)).isEmpty();
  }
}
