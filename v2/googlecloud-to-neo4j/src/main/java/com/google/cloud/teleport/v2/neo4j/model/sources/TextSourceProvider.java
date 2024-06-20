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
package com.google.cloud.teleport.v2.neo4j.model.sources;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.util.List;
import org.neo4j.importer.v1.sources.SourceProvider;

public class TextSourceProvider implements SourceProvider<TextSource> {

  private static final String DEFAULT_COLUMN_DELIMITER = ",";
  private static final String DEFAULT_LINE_SEPARATOR = "\n";

  private final YAMLMapper mapper;

  public TextSourceProvider() {
    mapper = YAMLMapper.builder().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS).build();
  }

  @Override
  public String supportedType() {
    return "text";
  }

  @Override
  public TextSource provide(ObjectNode node) {
    List<String> header = mapper.convertValue(node.get("header"), new TypeReference<>() {});
    if (node.has("urls")) {
      List<String> urls = mapper.convertValue(node.get("urls"), new TypeReference<>() {});
      TextFormat format = mapper.convertValue(node.get("format"), new TypeReference<>() {});
      return new ExternalTextSource(
          node.get("name").textValue(),
          urls,
          header,
          format,
          node.has("column_delimiter")
              ? node.get("column_delimiter").textValue()
              : DEFAULT_COLUMN_DELIMITER,
          node.has("line_separator")
              ? node.get("line_separator").textValue()
              : DEFAULT_LINE_SEPARATOR);
    }
    List<List<Object>> data = mapper.convertValue(node.get("data"), new TypeReference<>() {});
    return new InlineTextSource(node.get("name").textValue(), data, header);
  }
}
