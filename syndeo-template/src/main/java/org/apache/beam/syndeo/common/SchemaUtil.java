/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.syndeo.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

/* Utility methods relating to schemas. */
public class SchemaUtil {
  /** Return a row to the given schema, filling in null values if items are missing. */
  public static Row addNullsToMatchSchema(Row row, Schema schema) {
    if (schema.equivalent(row.getSchema())) return row;
    Row.Builder builder = Row.withSchema(schema);
    for (Field field : schema.getFields()) {
      if (row.getSchema().hasField(field.getName())) {
        builder.addValue(row.getValue(field.getName()));
      } else {
        if (!field.getType().getNullable()) {
          throw new IllegalArgumentException("Missing non-nullable field.");
        }
        builder.addValue(null);
      }
    }
    return builder.build();
  }

  /** Returns the schema of a SchemaTransform based on the input schema. */
  public static Map<String, Schema> getSchema(
      SchemaTransform transform, Map<String, Schema> inputSchemas) {
    Pipeline p = Pipeline.create();
    PCollectionRowTuple inputTuple = PCollectionRowTuple.empty(p);
    for (Entry<String, Schema> entry : inputSchemas.entrySet()) {
      inputTuple =
          inputTuple.and(entry.getKey(), p.apply(Create.empty(RowCoder.of(entry.getValue()))));
    }
    PCollectionRowTuple outputTuple = inputTuple.apply(transform.buildTransform());
    Map<String, Schema> outputSchemas = new HashMap<>();
    for (Entry<String, PCollection<Row>> entry : outputTuple.getAll().entrySet()) {
      outputSchemas.put(entry.getKey(), entry.getValue().getSchema());
    }
    return outputSchemas;
  }
}
