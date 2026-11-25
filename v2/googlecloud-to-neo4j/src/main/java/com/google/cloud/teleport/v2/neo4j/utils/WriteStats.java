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
package com.google.cloud.teleport.v2.neo4j.utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.SummaryCounters;

/** Beam helper for Neo4j write statistics records. */
public final class WriteStats {

  private static final Schema SCHEMA =
      Schema.of(
          Schema.Field.of("step", Schema.FieldType.STRING),
          Schema.Field.of("batch_rows", Schema.FieldType.INT64),
          Schema.Field.of("contains_updates", Schema.FieldType.BOOLEAN),
          Schema.Field.of("contains_system_updates", Schema.FieldType.BOOLEAN),
          Schema.Field.of("nodes_created", Schema.FieldType.INT64),
          Schema.Field.of("nodes_deleted", Schema.FieldType.INT64),
          Schema.Field.of("relationships_created", Schema.FieldType.INT64),
          Schema.Field.of("relationships_deleted", Schema.FieldType.INT64),
          Schema.Field.of("properties_set", Schema.FieldType.INT64),
          Schema.Field.of("labels_added", Schema.FieldType.INT64),
          Schema.Field.of("labels_removed", Schema.FieldType.INT64),
          Schema.Field.of("indexes_added", Schema.FieldType.INT64),
          Schema.Field.of("indexes_removed", Schema.FieldType.INT64),
          Schema.Field.of("constraints_added", Schema.FieldType.INT64),
          Schema.Field.of("constraints_removed", Schema.FieldType.INT64),
          Schema.Field.of("system_updates", Schema.FieldType.INT64));

  public static RowCoder coder() {
    return RowCoder.of(schema());
  }

  /**
   * Converts a write transaction's result summary into a statistics row.
   *
   * @param summary the summary returned by {@code Result#consume()}, which is never {@code null}
   */
  public static Row convertToRow(String stepName, int batchSize, ResultSummary summary) {
    SummaryCounters counters = summary.counters();
    Map<String, Object> values = new HashMap<>();
    values.put("step", stepName);
    values.put("batch_rows", (long) batchSize);
    values.put("contains_updates", counters.containsUpdates());
    values.put("contains_system_updates", counters.containsSystemUpdates());
    values.put("nodes_created", (long) counters.nodesCreated());
    values.put("nodes_deleted", (long) counters.nodesDeleted());
    values.put("relationships_created", (long) counters.relationshipsCreated());
    values.put("relationships_deleted", (long) counters.relationshipsDeleted());
    values.put("properties_set", (long) counters.propertiesSet());
    values.put("labels_added", (long) counters.labelsAdded());
    values.put("labels_removed", (long) counters.labelsRemoved());
    values.put("indexes_added", (long) counters.indexesAdded());
    values.put("indexes_removed", (long) counters.indexesRemoved());
    values.put("constraints_added", (long) counters.constraintsAdded());
    values.put("constraints_removed", (long) counters.constraintsRemoved());
    values.put("system_updates", (long) counters.systemUpdates());
    return Row.withSchema(schema()).withFieldValues(values).build();
  }

  public static Schema schema() {
    return SCHEMA;
  }
}
