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
package com.google.cloud.teleport.v2.templates.spanner;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.templates.model.SpannerSinkConfig;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerConfigFileReaderTest {

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  private SpannerConfigFileReader reader;

  @Before
  public void setUp() {
    reader = new SpannerConfigFileReader();
  }

  @Test
  public void testGetSpannerConfig_validJson_success() throws IOException {
    String json =
        "{\n"
            + "  \"projectId\": \"my-project\",\n"
            + "  \"instanceId\": \"my-instance\",\n"
            + "  \"databaseId\": \"my-database\",\n"
            + "  \"dialect\": \"POSTGRESQL\"\n"
            + "}";
    File file = tempFolder.newFile("spanner_config.json");
    FileUtils.writeStringToFile(file, json, StandardCharsets.UTF_8);

    SpannerSinkConfig config = reader.getSpannerConfig(file.getAbsolutePath());

    assertThat(config).isNotNull();
    assertThat(config.getProjectId()).isEqualTo("my-project");
    assertThat(config.getInstanceId()).isEqualTo("my-instance");
    assertThat(config.getDatabaseId()).isEqualTo("my-database");
    assertThat(config.getDialect()).isEqualTo(Dialect.POSTGRESQL);
  }

  @Test
  public void testGetSpannerConfig_nullConfig_throwsException() throws IOException {
    // Gson.fromJson on `"null"` returns null
    File file = tempFolder.newFile("spanner_config_null.json");
    FileUtils.writeStringToFile(file, "null", StandardCharsets.UTF_8);

    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> reader.getSpannerConfig(file.getAbsolutePath()));
    assertThat(ex).hasMessageThat().contains("resulting config is null");
  }

  @Test
  public void testGetSpannerConfig_missingProjectId_throwsException() throws IOException {
    String json =
        "{\n"
            + "  \"instanceId\": \"my-instance\",\n"
            + "  \"databaseId\": \"my-database\"\n"
            + "}";
    File file = tempFolder.newFile("spanner_config_missing_project.json");
    FileUtils.writeStringToFile(file, json, StandardCharsets.UTF_8);

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> reader.getSpannerConfig(file.getAbsolutePath()));
    assertThat(ex).hasMessageThat().contains("Missing projectId");
  }

  @Test
  public void testGetSpannerConfig_missingInstanceId_throwsException() throws IOException {
    String json =
        "{\n" + "  \"projectId\": \"my-project\",\n" + "  \"databaseId\": \"my-database\"\n" + "}";
    File file = tempFolder.newFile("spanner_config_missing_instance.json");
    FileUtils.writeStringToFile(file, json, StandardCharsets.UTF_8);

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> reader.getSpannerConfig(file.getAbsolutePath()));
    assertThat(ex).hasMessageThat().contains("Missing instanceId");
  }

  @Test
  public void testGetSpannerConfig_missingDatabaseId_throwsException() throws IOException {
    String json =
        "{\n" + "  \"projectId\": \"my-project\",\n" + "  \"instanceId\": \"my-instance\"\n" + "}";
    File file = tempFolder.newFile("spanner_config_missing_database.json");
    FileUtils.writeStringToFile(file, json, StandardCharsets.UTF_8);

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> reader.getSpannerConfig(file.getAbsolutePath()));
    assertThat(ex).hasMessageThat().contains("Missing databaseId");
  }

  @Test
  public void testGetSpannerConfig_ioError_throwsException() {
    assertThrows(
        RuntimeException.class, () -> reader.getSpannerConfig("/non/existent/path/config.json"));
  }
}
