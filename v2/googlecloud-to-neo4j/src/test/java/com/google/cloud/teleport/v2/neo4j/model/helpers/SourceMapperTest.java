/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.model.helpers;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.model.sources.BigQuerySource;
import com.google.cloud.teleport.v2.neo4j.model.sources.ExternalTextSource;
import com.google.cloud.teleport.v2.neo4j.model.sources.InlineTextSource;
import com.google.cloud.teleport.v2.neo4j.model.sources.TextFormat;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Test;
import org.neo4j.importer.v1.sources.Source;

@SuppressWarnings("deprecation")
public class SourceMapperTest {

  @Test
  public void trims_source_fields() {
    JSONObject json = new JSONObject();
    json.put("name", "placeholder");
    json.put("type", "text");
    json.put("ordered_field_names", "foo, bar,   qix\t\r");
    json.put("data", "foovalue,barvalue,qixvalue");

    Source source = SourceMapper.parse(json, new OptionsParams());

    assertThat(source).isInstanceOf(InlineTextSource.class);
    InlineTextSource inlineTextSource = (InlineTextSource) source;
    assertThat(inlineTextSource.getHeader()).isEqualTo(List.of("foo", "bar", "qix"));
  }

  @Test
  public void parses_minimal_BigQuery_source() {
    var json =
        new JSONObject(
            Map.of(
                "type", "bigquery",
                "query", "SELECT 42"));

    Source source = SourceMapper.parse(json, new OptionsParams());

    assertThat(source).isEqualTo(new BigQuerySource("", "SELECT 42"));
  }

  @Test
  public void parses_BigQuery_source() {
    var json =
        new JSONObject(
            Map.of(
                "type", "bigquery",
                "query", "SELECT name FROM $table"));

    OptionsParams options = new OptionsParams();
    options.overlayTokens("{\"table\": \"placeholder-table\"}");

    Source source = SourceMapper.parse(json, options);

    assertThat(source).isEqualTo(new BigQuerySource("", "SELECT name FROM placeholder-table"));
  }

  @Test
  public void parses_minimal_external_text_source() {
    var json =
        new JSONObject(
            Map.of(
                "ordered_field_names", "col1,col2,col3",
                "uri", "https://example.com"));

    Source source = SourceMapper.parse(json, new OptionsParams());

    assertThat(source)
        .isEqualTo(
            new ExternalTextSource(
                "",
                List.of("https://example.com"),
                List.of("col1", "col2", "col3"),
                TextFormat.DEFAULT,
                ",",
                null));
  }

  @Test
  public void parses_external_text_source() {
    var json =
        new JSONObject(
            Map.of(
                "name", "a-source",
                "format", "EXCEL",
                "delimiter", "%",
                "separator", "=",
                "ordered_field_names", "col1,col2,col3",
                "url", "https://example.$ext"));
    OptionsParams options = new OptionsParams();
    options.overlayTokens("{\"ext\": \"com\"}");

    Source source = SourceMapper.parse(json, options);

    assertThat(source)
        .isEqualTo(
            new ExternalTextSource(
                "a-source",
                List.of("https://example.com"),
                List.of("col1", "col2", "col3"),
                TextFormat.EXCEL,
                "%",
                "="));
  }

  @Test
  public void parses_minimal_inline_text_source() {
    var json =
        new JSONObject(
            Map.of(
                "ordered_field_names", "col1,col2,col3",
                "data", "value1,value2,value3\nvalue4,value5,value6"));

    Source source = SourceMapper.parse(json, new OptionsParams());

    assertThat(source)
        .isEqualTo(
            new InlineTextSource(
                "",
                List.of(
                    List.of("value1", "value2", "value3"), List.of("value4", "value5", "value6")),
                List.of("col1", "col2", "col3")));
  }

  @Test
  public void parses_inline_text_source() {
    var json =
        new JSONObject(
            Map.of(
                "ordered_field_names", "col1,col2,col3",
                "format", "EXCEL",
                "data",
                    List.of(
                        List.of("value1", "value2", "value3"),
                        List.of("value4", "value5", "value6"))));

    Source source = SourceMapper.parse(json, new OptionsParams());

    assertThat(source)
        .isEqualTo(
            new InlineTextSource(
                "",
                List.of(
                    List.of("value1", "value2", "value3"), List.of("value4", "value5", "value6")),
                List.of("col1", "col2", "col3")));
  }
}
