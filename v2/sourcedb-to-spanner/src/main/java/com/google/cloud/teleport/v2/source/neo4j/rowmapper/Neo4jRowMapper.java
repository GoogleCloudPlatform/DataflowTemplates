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
package com.google.cloud.teleport.v2.source.neo4j.rowmapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;

/** Custom row mapper to translate Neo4j Records into Avro-based SourceRows. */
public class Neo4jRowMapper implements Serializable {

  private final SourceSchemaReference sourceSchemaReference;
  private final SourceTableSchema sourceTableSchema;
  private final ObjectMapper objectMapper;

  public Neo4jRowMapper(
      SourceSchemaReference sourceSchemaReference, SourceTableSchema sourceTableSchema) {
    this.sourceSchemaReference = sourceSchemaReference;
    this.sourceTableSchema = sourceTableSchema;
    this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
  }

  private long getCurrentTimeMicros() {
    Instant now = Instant.now();
    return TimeUnit.NANOSECONDS.toMicros(
        TimeUnit.SECONDS.toNanos(now.getEpochSecond()) + now.getNano());
  }

  public SourceRow map(Record record) throws Exception {
    long time = getCurrentTimeMicros();
    SourceRow.Builder sourceRowBuilder =
        SourceRow.builder(sourceSchemaReference, sourceTableSchema, null, time);

    String tableName = sourceTableSchema.tableName();
    if ("GraphNode".equals(tableName)) {
      sourceRowBuilder.setField("id", String.valueOf(record.get("id").asLong()));

      List<String> labels = record.get("labels").asList(Value::asString);
      String label = labels.isEmpty() ? "graphnode" : labels.get(0).toLowerCase();
      sourceRowBuilder.setField("label", label);

      Map<String, Object> props = record.get("properties").asMap();
      sourceRowBuilder.setField("properties", toLowercaseJson(props));

    } else if ("GraphEdge".equals(tableName)) {
      sourceRowBuilder.setField("id", String.valueOf(record.get("src_id").asLong()));
      sourceRowBuilder.setField("dest_id", String.valueOf(record.get("dest_id").asLong()));
      sourceRowBuilder.setField("edge_id", String.valueOf(record.get("edge_id").asLong()));

      String label = record.get("label").asString().toLowerCase();
      sourceRowBuilder.setField("label", label);

      Map<String, Object> props = record.get("properties").asMap();
      sourceRowBuilder.setField("properties", toLowercaseJson(props));
    } else {
      throw new IllegalArgumentException("Unknown virtual Neo4j table: " + tableName);
    }

    return sourceRowBuilder.build();
  }

  private String toLowercaseJson(Map<String, Object> map) throws Exception {
    Map<String, Object> lowercaseMap = new HashMap<>();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      lowercaseMap.put(entry.getKey().toLowerCase(), entry.getValue());
    }
    return objectMapper.writeValueAsString(lowercaseMap);
  }
}
