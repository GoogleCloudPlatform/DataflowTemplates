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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Optional;
import org.neo4j.importer.v1.sources.SourceProvider;

public class BigQuerySourceProvider implements SourceProvider<BigQuerySource> {
  @Override
  public String supportedType() {
    return "bigquery";
  }

  @Override
  public BigQuerySource provide(ObjectNode node) {
    return new BigQuerySource(
        node.get("name").textValue(),
        node.get("query").textValue(),
        Optional.ofNullable(node.get("query_temp_project")).map(JsonNode::textValue).orElse(null),
        Optional.ofNullable(node.get("query_temp_dataset")).map(JsonNode::textValue).orElse(null));
  }
}
