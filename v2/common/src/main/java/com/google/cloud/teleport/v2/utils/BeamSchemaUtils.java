/*
 * Copyright (C) 2021 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.utils;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.fromTableSchema;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.toTableSchema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;

/**
 * BeamSchemaUtils has utilities scope for convert {@link Schema} into/from various formats
 */
public class BeamSchemaUtils {

  public static final String FIELD_NAME = "name";
  public static final String FIELD_TYPE = "type";

  /**
   * Convert {@link Schema} into json string
   *
   * @param beamSchema {@link Schema}
   * @return json string as {@link String}
   */
  public static String beamSchemaToJson(Schema beamSchema) {
    ObjectMapper mapper = new ObjectMapper();
    ArrayNode beamSchemaJsonNode = mapper.createArrayNode();

    for (Field field : beamSchema.getFields()) {
      ObjectNode fieldJsonNode = mapper.createObjectNode();
      fieldJsonNode.put(FIELD_NAME, field.getName());
      fieldJsonNode.put(FIELD_TYPE, field.getType().getTypeName().toString());

      beamSchemaJsonNode.add(fieldJsonNode);
    }

    return beamSchemaJsonNode.toString();
  }

  /**
   * Convert a BigQuery {@link TableSchema} to a Beam {@link Schema}.
   */
  public static Schema bigQuerySchemaToBeamSchema(TableSchema bigQuerySchema) {
    return fromTableSchema(bigQuerySchema);
  }

  /**
   * Convert a Beam {@link Schema} to a BigQuery {@link TableSchema}.
   */
  public static TableSchema beamSchemaToBigQuerySchema(Schema beamSchema) {
    return toTableSchema(beamSchema);
  }

}
