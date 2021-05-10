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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Enums;
import com.google.common.base.Optional;
import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;



/**
 * BeamSchemaUtils has utilities scope for convert {@link Schema} into/from various formats
 */
public class BeamSchemaUtils {

  public static final String FIELD_NAME = "name";
  public static final String FIELD_TYPE = "type";
  static final JsonFactory FACTORY = new JsonFactory();
  static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

  /**
   * Convert {@link Schema} into json string
   *
   * @param beamSchema {@link Schema}
   * @return json string as {@link String}
   */
  public static String beamSchemaToJson(Schema beamSchema) {
    ArrayNode beamSchemaJsonNode = MAPPER.createArrayNode();

    for (Field field : beamSchema.getFields()) {
      ObjectNode fieldJsonNode = MAPPER.createObjectNode();
      fieldJsonNode.put(FIELD_NAME, field.getName());
      fieldJsonNode.put(FIELD_TYPE, field.getType().getTypeName().toString());

      beamSchemaJsonNode.add(fieldJsonNode);
    }

    return beamSchemaJsonNode.toString();
  }

  /**
   * Parse provided json string to {@link Schema}.
   *
   * <p>Json string should be in "[{"type": "INT32", "name": "fieldName"}, ...]" format
   *
   * @param jsonString json compatible string
   * @return converted {@link Schema}
   */
  public static Schema fromJson(String jsonString) throws SchemaParseException, IOException {
    try (JsonParser jsonParser = FACTORY.createParser(new StringReader(jsonString))) {
      return fromJson(jsonParser);
    }
  }


  private static Schema fromJson(JsonParser jsonParser) throws IOException, SchemaParseException {
    JsonNode jsonNodes = MAPPER.readTree(jsonParser);
    if (!jsonNodes.isArray()) {
      throw new SchemaParseException(
          "Provided schema must be in \"[{\"type\": \"fieldTypeName\", \"name\": \"fieldName\"}, ...]\" format");
    }
    return new Schema(getFieldsfromJsonNode(jsonNodes));

  }


  private static List<Field> getFieldsfromJsonNode(JsonNode jsonNode)
      throws SchemaParseException {
    List<Field> fields = new LinkedList<>();
    for (JsonNode node : jsonNode) {
      if (!node.isObject()) {
        throw new SchemaParseException("Node must be object: " + node.toString());
      }
      String type = getText(node, FIELD_TYPE, "type is missed");
      String name = getText(node, FIELD_NAME, "name is missed");
      fields.add(Field.of(name, stringToFieldType(type)));
    }
    return fields;
  }

  private static FieldType stringToFieldType(String string) throws SchemaParseException {
    Optional<TypeName> typeName = Enums.getIfPresent(TypeName.class, string);
    if (!typeName.isPresent()) {
      throw new SchemaParseException(String.format("Provided type \"%s\" does not exist", string));
    }
    return FieldType.of(typeName.get());
  }

  private static String getOptionalText(JsonNode node, String key) {
    JsonNode jsonNode = node.get(key);
    return jsonNode != null ? jsonNode.textValue() : null;
  }

  private static String getText(JsonNode node, String key, String errorMessage) {
    String text = getOptionalText(node, key);
    checkNotNull(text, errorMessage + ": " + node);
    return text;
  }

  public static class SchemaParseException extends Exception {

    public SchemaParseException(Throwable cause) {
      super(cause);
    }

    public SchemaParseException(String message) {
      super(message);
    }
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
