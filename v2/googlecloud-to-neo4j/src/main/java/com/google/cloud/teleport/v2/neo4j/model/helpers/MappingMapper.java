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

import static com.google.cloud.teleport.v2.neo4j.model.helpers.JsonObjects.getBooleanOrDefault;
import static com.google.cloud.teleport.v2.neo4j.model.helpers.JsonObjects.getStringOrDefault;
import static com.google.cloud.teleport.v2.neo4j.model.helpers.JsonObjects.getStringOrNull;
import static com.google.cloud.teleport.v2.neo4j.model.helpers.MappingVisitor.visit;
import static com.google.cloud.teleport.v2.neo4j.model.helpers.SourceMapper.DEFAULT_SOURCE_NAME;
import static org.neo4j.importer.v1.targets.PropertyType.BOOLEAN;
import static org.neo4j.importer.v1.targets.PropertyType.BYTE_ARRAY;
import static org.neo4j.importer.v1.targets.PropertyType.FLOAT;
import static org.neo4j.importer.v1.targets.PropertyType.INTEGER;
import static org.neo4j.importer.v1.targets.PropertyType.POINT;
import static org.neo4j.importer.v1.targets.PropertyType.STRING;
import static org.neo4j.importer.v1.targets.PropertyType.ZONED_DATETIME;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.importer.v1.targets.NodeExistenceConstraint;
import org.neo4j.importer.v1.targets.NodeKeyConstraint;
import org.neo4j.importer.v1.targets.NodeRangeIndex;
import org.neo4j.importer.v1.targets.NodeSchema;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.NodeUniqueConstraint;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.RelationshipExistenceConstraint;
import org.neo4j.importer.v1.targets.RelationshipKeyConstraint;
import org.neo4j.importer.v1.targets.RelationshipRangeIndex;
import org.neo4j.importer.v1.targets.RelationshipSchema;
import org.neo4j.importer.v1.targets.RelationshipUniqueConstraint;
import org.neo4j.importer.v1.targets.Target;
import org.neo4j.importer.v1.targets.WriteMode;

/**
 * Helper object for parsing legacy json for property mappings, schema, labels and types.
 *
 * @deprecated use the current JSON format instead
 */
@Deprecated
class MappingMapper {

  public static String parseType(JSONObject mappings) {
    return unquote(getStringOrNull(mappings, "type"));
  }

  /**
   * This tries to match an embedded edge node spec against an existing node target. A node targets
   * matches if and only if it passes all the following criteria:<br>
   * - its labels are the same as the embedded node's<br>
   * - its property mappings form a superset of the embedded node's<br>
   * - its key constraints match all the embedded node's by comparing label & property names<br>
   *
   * @param edgeNode the legacy JSON representation of the start or end node of the edge
   * @param nodes all the processed node targets
   * @return the matching target name or an empty optional
   */
  public static Optional<String> findNodeTarget(JSONObject edgeNode, List<NodeTarget> nodes) {
    var labels = parseLabels(edgeNode);
    var mappings = parseKeyMappings(edgeNode);
    var keyDefinitions =
        nodeKeyDefinitionsOf(parseNodeKeyConstraints("irrelevant", labels, edgeNode));
    return nodes.stream()
        .filter(
            target ->
                labels.equals(target.getLabels())
                    && new HashSet<>(target.getProperties()).containsAll(mappings)
                    && nodeKeyDefinitionsOf(target.getSchema()).containsAll(keyDefinitions))
        .map(Target::getName)
        .findFirst();
  }

  public static NodeTarget parseEdgeNode(JSONObject edge, String key, WriteMode writeMode) {
    var node = edge.getJSONObject("mappings").getJSONObject(key);
    var targetName = String.format("%s-%s", edge.getString("name"), key);
    var labels = parseLabels(node);

    Map<String, PropertyMapping> keyMappings = new LinkedHashMap<>();
    List<NodeKeyConstraint> keyConstraints = new ArrayList<>();
    var propertyListener = new PropertyMappingListener(keyMappings, MappingMapper::untypedMapping);
    if (node.has("key")) {
      var keyListener = new SingleNodeKeyConstraintListener(targetName, labels);
      visit(node.get("key"), MappingListeners.of(propertyListener, keyListener));
      keyConstraints.addAll(keyListener.getSchema());
    }
    if (node.has("keys")) {
      var keysListener = new NodeKeyConstraintsListener(targetName, labels);
      visit(node.get("keys"), MappingListeners.of(propertyListener, keysListener));
      keyConstraints.addAll(keysListener.getSchema());
    }
    return new NodeTarget(
        getBooleanOrDefault(edge, "active", true), // inherit active status from enclosing edge
        targetName,
        getStringOrDefault(edge, "source", DEFAULT_SOURCE_NAME),
        null,
        writeMode,
        null,
        labels,
        new ArrayList<>(keyMappings.values()),
        new NodeSchema(null, keyConstraints, null, null, null, null, null, null, null));
  }

  public static List<String> parseLabels(JSONObject mappings) {
    // breaking: this implementation drops support for labels specified as a JSONObject because it
    // does not make much sense
    List<String> labels = new ArrayList<>();
    if (mappings.has("label")) {
      labels.addAll(parseLabelStringOrArray(mappings.get("label")));
    }
    if (mappings.has("labels")) {
      labels.addAll(parseLabelStringOrArray(mappings.get("labels")));
    }
    return labels;
  }

  public static List<PropertyMapping> parseMappings(JSONObject mappings) {
    if (!mappings.has("properties")) {
      return List.of();
    }
    JSONObject properties = mappings.getJSONObject("properties");
    Map<String, PropertyMapping> indexedMappings = new LinkedHashMap<>();
    var mappingListener =
        new PropertyMappingListener(indexedMappings, MappingMapper::untypedMapping);
    if (properties.has("key")) {
      visit(properties.get("key"), mappingListener);
    }
    if (properties.has("keys")) {
      visit(properties.get("keys"), mappingListener);
    }
    if (properties.has("unique")) {
      visit(properties.get("unique"), mappingListener);
    }
    if (properties.has("mandatory")) {
      visit(properties.get("mandatory"), mappingListener);
    }
    if (properties.has("indexed")) {
      visit(properties.get("indexed"), mappingListener);
    }
    if (properties.has("dates")) {
      var listener =
          new PropertyMappingListener(
              indexedMappings,
              (field, property) -> new PropertyMapping(field, property, ZONED_DATETIME));
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
      String targetName,
      List<String> labels,
      JSONObject mappings,
      List<String> defaultIndexedProperties) {

    if (!mappings.has("properties")) {
      return null;
    }
    JSONObject properties = mappings.getJSONObject("properties");
    List<NodeKeyConstraint> keyConstraints =
        parseNodeKeyConstraints(targetName, labels, properties);
    List<NodeUniqueConstraint> uniqueConstraints = new ArrayList<>(0);
    if (properties.has("unique")) {
      var uniqueConstraintListener = new NodeUniqueConstraintsListener(targetName, labels);
      visit(properties.get("unique"), uniqueConstraintListener);
      uniqueConstraints = uniqueConstraintListener.getSchema();
    }
    List<NodeExistenceConstraint> existenceConstraints = new ArrayList<>(0);
    if (properties.has("mandatory")) {
      var existenceConstraintListener = new NodeExistenceConstraintListener(targetName, labels);
      visit(properties.get("mandatory"), existenceConstraintListener);
      existenceConstraints = existenceConstraintListener.getSchema();
    }
    List<NodeRangeIndex> indexes = new ArrayList<>(0);
    if (properties.has("indexed")) {
      CompoundNodeSchemaListener<NodeRangeIndex> indexListener =
          new NodeIndexListener(targetName, labels);
      visit(properties.get("indexed"), indexListener);
      indexes.addAll(indexListener.getSchema());
    }
    if (!defaultIndexedProperties.isEmpty()) {
      indexes.addAll(
          generateAutomaticIndexes(
              labels, defaultIndexedProperties, keyConstraints, uniqueConstraints, indexes));
    }
    return new NodeSchema(
        null,
        nullIfEmpty(keyConstraints),
        nullIfEmpty(uniqueConstraints),
        nullIfEmpty(existenceConstraints),
        nullIfEmpty(indexes),
        null,
        null,
        null,
        null);
  }

  public static RelationshipSchema parseEdgeSchema(
      String targetName, String type, JSONObject mappings, List<String> defaultIndexedProperties) {

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
    List<RelationshipUniqueConstraint> uniqueConstraints = new ArrayList<>(0);
    if (properties.has("unique")) {
      var uniqueConstraintListener = new RelationshipUniqueConstraintsListener(targetName, type);
      visit(properties.get("unique"), uniqueConstraintListener);
      uniqueConstraints = uniqueConstraintListener.getSchema();
    }
    List<RelationshipExistenceConstraint> existenceConstraints = new ArrayList<>(0);
    if (properties.has("mandatory")) {
      var existenceConstraintListener =
          new RelationshipExistenceConstraintListener(targetName, type);
      visit(properties.get("mandatory"), existenceConstraintListener);
      existenceConstraints = existenceConstraintListener.getSchema();
    }
    List<RelationshipRangeIndex> indexes = new ArrayList<>(0);
    if (properties.has("indexed")) {
      var indexListener = new RelationshipIndexListener(targetName, type);
      visit(properties.get("indexed"), indexListener);
      indexes.addAll(indexListener.getSchema());
    }
    if (!defaultIndexedProperties.isEmpty()) {
      indexes.addAll(
          generateAutomaticIndexes(
              type, defaultIndexedProperties, keyConstraints, uniqueConstraints, indexes));
    }
    return new RelationshipSchema(
        null,
        nullIfEmpty(keyConstraints),
        nullIfEmpty(uniqueConstraints),
        nullIfEmpty(existenceConstraints),
        nullIfEmpty(indexes),
        null,
        null,
        null,
        null);
  }

  private static List<String> parseLabelStringOrArray(Object rawLabels) {
    List<String> labels = new ArrayList<>();
    if (rawLabels instanceof String) {
      String rawLabel = (String) rawLabels;
      labels.add(unquote(rawLabel));
    } else if (rawLabels instanceof JSONArray) {
      JSONArray jsonLabels = (JSONArray) rawLabels;
      for (int i = 0; i < jsonLabels.length(); i++) {
        labels.add(unquote(jsonLabels.getString(i)));
      }
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported type for label(s), expected string or array of strings, got: %s",
              rawLabels.getClass()));
    }
    return labels;
  }

  private static String unquote(String string) {
    if (string == null) {
      return null;
    }
    String value = string.trim();
    if (value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"') {
      return value.substring(1, value.length() - 1);
    }
    return value;
  }

  private static PropertyMapping untypedMapping(String field, String property) {
    return new PropertyMapping(field, property, null);
  }

  private static Collection<PropertyMapping> parseKeyMappings(JSONObject node) {
    Map<String, PropertyMapping> keyMappings = new LinkedHashMap<>();
    var propertyListener = new PropertyMappingListener(keyMappings, MappingMapper::untypedMapping);
    if (node.has("key")) {
      visit(node.get("key"), propertyListener);
    }
    if (node.has("keys")) {
      visit(node.get("keys"), propertyListener);
    }
    return keyMappings.values();
  }

  private static List<NodeRangeIndex> generateAutomaticIndexes(
      List<String> labels,
      List<String> defaultIndexedProperties,
      List<NodeKeyConstraint> keyConstraints,
      List<NodeUniqueConstraint> uniqueConstraints,
      List<NodeRangeIndex> indexes) {
    var properties = new ArrayList<>(defaultIndexedProperties);
    properties.removeAll(
        keyConstraints.stream()
            .flatMap(constraint -> constraint.getProperties().stream())
            .distinct()
            .collect(Collectors.toList()));
    properties.removeAll(
        uniqueConstraints.stream()
            .flatMap(constraint -> constraint.getProperties().stream())
            .distinct()
            .collect(Collectors.toList()));
    properties.removeAll(
        indexes.stream()
            .flatMap(index -> index.getProperties().stream())
            .distinct()
            .collect(Collectors.toList()));
    return properties.stream()
        .flatMap(property -> labels.stream().map(label -> automaticNodeIndex(label, property)))
        .collect(Collectors.toList());
  }

  private static List<RelationshipRangeIndex> generateAutomaticIndexes(
      String type,
      List<String> defaultIndexedProperties,
      List<RelationshipKeyConstraint> keyConstraints,
      List<RelationshipUniqueConstraint> uniqueConstraints,
      List<RelationshipRangeIndex> indexes) {
    var properties = new ArrayList<>(defaultIndexedProperties);
    properties.removeAll(
        keyConstraints.stream()
            .flatMap(constraint -> constraint.getProperties().stream())
            .distinct()
            .collect(Collectors.toList()));
    properties.removeAll(
        uniqueConstraints.stream()
            .flatMap(constraint -> constraint.getProperties().stream())
            .distinct()
            .collect(Collectors.toList()));
    properties.removeAll(
        indexes.stream()
            .flatMap(index -> index.getProperties().stream())
            .distinct()
            .collect(Collectors.toList()));
    return properties.stream()
        .map(property -> automaticRelationshipIndex(type, property))
        .collect(Collectors.toList());
  }

  private static NodeRangeIndex automaticNodeIndex(String label, String property) {
    return new NodeRangeIndex(
        String.format("node/default-index-for-%s-%s", label, property), label, List.of(property));
  }

  private static RelationshipRangeIndex automaticRelationshipIndex(String type, String property) {
    return new RelationshipRangeIndex(
        String.format("edge/default-index-for-%s-%s", type, property), List.of(property));
  }

  private static <T> List<T> nullIfEmpty(List<T> items) {
    return items.isEmpty() ? null : items;
  }

  private static List<NodeKeyConstraint> parseNodeKeyConstraints(
      String targetName, List<String> labels, JSONObject properties) {
    List<NodeKeyConstraint> keyConstraints = new ArrayList<>();
    if (properties.has("key")) {
      var listener = new SingleNodeKeyConstraintListener(targetName, labels);
      visit(properties.get("key"), listener);
      keyConstraints.addAll(listener.getSchema());
    }
    if (properties.has("keys")) {
      var listener = new NodeKeyConstraintsListener(targetName, labels);
      visit(properties.get("keys"), listener);
      keyConstraints.addAll(listener.getSchema());
    }
    return keyConstraints;
  }

  private static Set<NodeKeyDefinition> nodeKeyDefinitionsOf(NodeSchema schema) {
    if (schema == null) {
      return Set.of();
    }
    var keyConstraints = schema.getKeyConstraints();
    if (keyConstraints == null) {
      return Set.of();
    }
    return nodeKeyDefinitionsOf(keyConstraints);
  }

  private static Set<NodeKeyDefinition> nodeKeyDefinitionsOf(
      List<NodeKeyConstraint> keyConstraints) {
    return keyConstraints.stream()
        .map(constraint -> new NodeKeyDefinition(constraint.getLabel(), constraint.getProperties()))
        .collect(Collectors.toSet());
  }
}

class NodeKeyDefinition {
  private final String label;
  private final List<String> properties;

  public NodeKeyDefinition(String label, List<String> properties) {
    this.label = label;
    this.properties = properties;
  }

  public String getLabel() {
    return label;
  }

  public List<String> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof NodeKeyDefinition)) {
      return false;
    }
    NodeKeyDefinition that = (NodeKeyDefinition) o;
    return Objects.equals(label, that.label) && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(label, properties);
  }
}
