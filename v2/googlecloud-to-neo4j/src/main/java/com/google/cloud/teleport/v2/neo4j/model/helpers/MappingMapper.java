/*
 * Copyright (C) 2022 Google LLC
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

import static com.google.cloud.teleport.v2.neo4j.model.helpers.JsonObjects.getStringOrNull;
import static com.google.cloud.teleport.v2.neo4j.model.helpers.MappingVisitor.visit;
import static org.neo4j.importer.v1.targets.PropertyType.BOOLEAN;
import static org.neo4j.importer.v1.targets.PropertyType.BYTE_ARRAY;
import static org.neo4j.importer.v1.targets.PropertyType.DATE;
import static org.neo4j.importer.v1.targets.PropertyType.FLOAT;
import static org.neo4j.importer.v1.targets.PropertyType.INTEGER;
import static org.neo4j.importer.v1.targets.PropertyType.POINT;
import static org.neo4j.importer.v1.targets.PropertyType.STRING;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.importer.v1.targets.NodeKeyConstraint;
import org.neo4j.importer.v1.targets.NodeRangeIndex;
import org.neo4j.importer.v1.targets.NodeSchema;
import org.neo4j.importer.v1.targets.NodeUniqueConstraint;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.RelationshipKeyConstraint;
import org.neo4j.importer.v1.targets.RelationshipNode;
import org.neo4j.importer.v1.targets.RelationshipRangeIndex;
import org.neo4j.importer.v1.targets.RelationshipSchema;
import org.neo4j.importer.v1.targets.RelationshipUniqueConstraint;

/**
 * Helper object for parsing legacy json for property mappings, schema, labels and types.
 *
 * @deprecated use the current JSON format instead
 */
@Deprecated
public class MappingMapper {

  public static List<String> parseLabels(JSONObject mappings) {
    // breaking: this implementation drops support for labels specified as a JSONObject because it
    // does not make much sense
    List<String> labels = new ArrayList<>();
    if (mappings.has("label")) {
      labels.add(parseLabel(mappings.getString("label")));
    }
    if (mappings.has("labels")) {
      Object rawLabels = mappings.get("labels");
      if (rawLabels instanceof String) {
        String rawLabel = (String) rawLabels;
        labels.add(parseLabel(rawLabel));
      } else if (rawLabels instanceof JSONArray) {
        JSONArray jsonLabels = (JSONArray) rawLabels;
        for (int i = 0; i < jsonLabels.length(); i++) {
          labels.add(parseLabel(jsonLabels.getString(i)));
        }
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Unsupported type for label(s), expected string or array of strings, got: %s",
                rawLabels.getClass()));
      }
    }
    return labels;
  }

  public static String parseType(JSONObject mappings) {
    return getStringOrNull(mappings, "type");
  }

  public static RelationshipNode parseEdgeNode(JSONObject mappings, String key) {
    if (!mappings.has(key)) {
      return null;
    }
    JSONObject node = mappings.getJSONObject(key);
    if (!node.has("key") && !node.has("keys")) {
      throw new IllegalArgumentException(
          String.format(
              "Edge node fragment of type %s should define a \"key\" or \"keys\" attribute. None found",
              key));
    }
    Map<String, PropertyMapping> indexedMappings = new LinkedHashMap<>();
    var untypedListener =
        new PropertyMappingListener(indexedMappings, MappingMapper::untypedMapping);
    if (node.has("key")) {
      visit(node.get("key"), untypedListener);
    }
    if (node.has("keys")) {
      visit(node.get("keys"), untypedListener);
    }
    return new RelationshipNode(
        // breaking: multiple labels under label were previously allowed, not anymore
        parseLabel(node.getString("label")),
        indexedMappings.isEmpty() ? null : new ArrayList<>(indexedMappings.values()));
  }

  public static List<PropertyMapping> parseMappings(JSONObject mappings) {
    if (!mappings.has("properties")) {
      return null;
    }
    JSONObject properties = mappings.getJSONObject("properties");
    Map<String, PropertyMapping> indexedMappings = new LinkedHashMap<>();
    var untypedListener =
        new PropertyMappingListener(indexedMappings, MappingMapper::untypedMapping);
    if (properties.has("key")) {
      visit(properties.get("key"), untypedListener);
    }
    if (properties.has("keys")) {
      visit(properties.get("keys"), untypedListener);
    }
    if (properties.has("unique")) {
      visit(properties.get("unique"), untypedListener);
    }
    if (properties.has("mandatory")) {
      visit(properties.get("mandatory"), untypedListener);
    }
    if (properties.has("indexed")) {
      visit(properties.get("indexed"), untypedListener);
    }
    if (properties.has("dates")) {
      var listener =
          new PropertyMappingListener(
              indexedMappings, (field, property) -> new PropertyMapping(field, property, DATE));
      visit(properties.get("dates"), listener);
    }
    if (properties.has("doubles")) {
      var listener =
          new PropertyMappingListener(
              indexedMappings, (field, property) -> new PropertyMapping(field, property, FLOAT));
      visit(properties.get("doubles"), listener);
    }
    if (properties.has("floats")) {
      var listener =
          new PropertyMappingListener(
              indexedMappings, (field, property) -> new PropertyMapping(field, property, FLOAT));
      visit(properties.get("floats"), listener);
    }
    if (properties.has("longs")) {
      var listener =
          new PropertyMappingListener(
              indexedMappings, (field, property) -> new PropertyMapping(field, property, INTEGER));
      visit(properties.get("longs"), listener);
    }
    if (properties.has("integers")) {
      var listener =
          new PropertyMappingListener(
              indexedMappings, (field, property) -> new PropertyMapping(field, property, INTEGER));
      visit(properties.get("integers"), listener);
    }
    if (properties.has("strings")) {
      var listener =
          new PropertyMappingListener(
              indexedMappings, (field, property) -> new PropertyMapping(field, property, STRING));
      visit(properties.get("strings"), listener);
    }
    if (properties.has("points")) {
      var listener =
          new PropertyMappingListener(
              indexedMappings, (field, property) -> new PropertyMapping(field, property, POINT));
      visit(properties.get("points"), listener);
    }
    if (properties.has("booleans")) {
      var listener =
          new PropertyMappingListener(
              indexedMappings, (field, property) -> new PropertyMapping(field, property, BOOLEAN));
      visit(properties.get("booleans"), listener);
    }
    if (properties.has("bytearrays")) {
      var listener =
          new PropertyMappingListener(
              indexedMappings,
              (field, property) -> new PropertyMapping(field, property, BYTE_ARRAY));
      visit(properties.get("bytearrays"), listener);
    }
    return new ArrayList<>(indexedMappings.values());
  }

  public static NodeSchema parseNodeSchema(
      String targetName, List<String> labels, JSONObject mappings) {
    if (!mappings.has("properties")) {
      return null;
    }
    JSONObject properties = mappings.getJSONObject("properties");
    List<NodeKeyConstraint> keyConstraints = new ArrayList<>();
    if (properties.has("key")) {
      SingleNodeKeyConstraintListener listener =
          new SingleNodeKeyConstraintListener(targetName, labels);
      visit(properties.get("key"), listener);
      keyConstraints.addAll(listener.getSchema());
    }
    if (properties.has("keys")) {
      CompoundNodeSchemaListener<NodeKeyConstraint> listener =
          new NodeKeyConstraintsListener(targetName, labels);
      visit(properties.get("keys"), listener);
      keyConstraints.addAll(listener.getSchema());
    }
    CompoundNodeSchemaListener<NodeUniqueConstraint> uniqueConstraintListener =
        new NodeUniqueConstraintsListener(targetName, labels);
    if (properties.has("unique")) {
      visit(properties.get("unique"), uniqueConstraintListener);
    }
    NodeExistenceConstraintListener existenceConstraintListener =
        new NodeExistenceConstraintListener(targetName, labels);
    if (properties.has("mandatory")) {
      visit(properties.get("mandatory"), existenceConstraintListener);
    }
    CompoundNodeSchemaListener<NodeRangeIndex> indexListener =
        new NodeIndexListener(targetName, labels);
    if (properties.has("indexed")) {
      visit(properties.get("indexed"), indexListener);
    }
    return new NodeSchema(
        false,
        keyConstraints.isEmpty() ? null : keyConstraints,
        uniqueConstraintListener.getSchema(),
        existenceConstraintListener.getSchema(),
        indexListener.getSchema(),
        null,
        null,
        null,
        null);
  }

  public static RelationshipSchema parseEdgeSchema(
      String targetName, String type, JSONObject mappings) {
    if (!mappings.has("properties")) {
      return null;
    }
    JSONObject properties = mappings.getJSONObject("properties");
    List<RelationshipKeyConstraint> keyConstraints = new ArrayList<>();
    if (properties.has("key")) {
      SingleRelationshipKeyConstraintListener listener =
          new SingleRelationshipKeyConstraintListener(targetName, type);
      visit(properties.get("key"), listener);
      keyConstraints.add(listener.getSchema());
    }
    if (properties.has("keys")) {
      CompoundRelationshipSchemaListener<RelationshipKeyConstraint> listener =
          new RelationshipKeyConstraintsListener(targetName, type);
      visit(properties.get("keys"), listener);
      keyConstraints.addAll(listener.getSchema());
    }
    CompoundRelationshipSchemaListener<RelationshipUniqueConstraint> uniqueConstraintListener =
        new RelationshipUniqueConstraintsListener(targetName, type);
    if (properties.has("unique")) {
      visit(properties.get("unique"), uniqueConstraintListener);
    }
    RelationshipExistenceConstraintListener existenceConstraintListener =
        new RelationshipExistenceConstraintListener(targetName, type);
    if (properties.has("mandatory")) {
      visit(properties.get("mandatory"), existenceConstraintListener);
    }
    CompoundRelationshipSchemaListener<RelationshipRangeIndex> indexListener =
        new RelationshipIndexListener(targetName, type);
    if (properties.has("indexed")) {
      visit(properties.get("indexed"), indexListener);
    }
    return new RelationshipSchema(
        false,
        keyConstraints.isEmpty() ? null : keyConstraints,
        uniqueConstraintListener.getSchema(),
        existenceConstraintListener.getSchema(),
        indexListener.getSchema(),
        null,
        null,
        null,
        null);
  }

  private static String parseLabel(String rawLabel) {
    String label = rawLabel.trim();
    if (label.charAt(0) == '"' && label.charAt(label.length() - 1) == '"') {
      return label.substring(1, label.length() - 1);
    }
    return label;
  }

  private static PropertyMapping untypedMapping(String field, String property) {
    return new PropertyMapping(field, property, null);
  }
}
