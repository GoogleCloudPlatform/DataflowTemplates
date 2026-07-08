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
package com.google.cloud.teleport.v2.neo4j.model.helpers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.neo4j.importer.v1.targets.NodeExistenceConstraint;
import org.neo4j.importer.v1.targets.NodeKeyConstraint;
import org.neo4j.importer.v1.targets.NodeRangeIndex;
import org.neo4j.importer.v1.targets.NodeUniqueConstraint;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.RelationshipExistenceConstraint;
import org.neo4j.importer.v1.targets.RelationshipKeyConstraint;
import org.neo4j.importer.v1.targets.RelationshipRangeIndex;
import org.neo4j.importer.v1.targets.RelationshipUniqueConstraint;

interface MappingListener {
  default void enterArray() {}

  default void exitArray() {}

  default void enterObject() {}

  default void enterObjectEntry(String field, String property) {}

  default void exitObject() {}

  default void enterString(String value) {}

  default void exitString() {}
}

class MappingListeners implements MappingListener {
  private final Collection<MappingListener> listeners;

  private MappingListeners(Collection<MappingListener> listeners) {
    this.listeners = listeners;
  }

  public static MappingListeners of(MappingListener first, MappingListener... rest) {
    var listeners = new ArrayList<MappingListener>();
    listeners.add(first);
    Collections.addAll(listeners, rest);
    return new MappingListeners(listeners);
  }

  @Override
  public void enterArray() {
    listeners.forEach(MappingListener::enterArray);
  }

  @Override
  public void exitArray() {
    listeners.forEach(MappingListener::exitArray);
  }

  @Override
  public void enterObject() {
    listeners.forEach(MappingListener::enterObject);
  }

  @Override
  public void enterObjectEntry(String field, String property) {
    listeners.forEach(listener -> listener.enterObjectEntry(field, property));
  }

  @Override
  public void exitObject() {
    listeners.forEach(MappingListener::exitObject);
  }

  @Override
  public void enterString(String value) {
    listeners.forEach(listener -> listener.enterString(value));
  }

  @Override
  public void exitString() {
    listeners.forEach(MappingListener::exitString);
  }
}

class PropertyMappingListener implements MappingListener {
  private final Map<String, PropertyMapping> mappingsByProperty;
  private final BiFunction<String, String, PropertyMapping> newMapping;

  PropertyMappingListener(
      Map<String, PropertyMapping> mappingsByProperty,
      BiFunction<String, String, PropertyMapping> newMapping) {

    this.mappingsByProperty = mappingsByProperty;
    this.newMapping = newMapping;
  }

  @Override
  public void enterObjectEntry(String field, String property) {
    mappingsByProperty.put(field, newMapping.apply(field, property));
  }

  @Override
  public void enterString(String value) {
    mappingsByProperty.put(value, newMapping.apply(value, value));
  }
}

/**
 * SingleNodeKeyConstraintListener groups mapping elements into a single key constraint definition
 * (per label). For instance, let us assume the spec defines a node target with 2 labels ("L1" and
 * "L2"). That target defines a key mapping as an array of 3 strings (for properties "p1", "p2" and
 * "p3"). This listener will generate 2 constraint definitions:<br>
 * - one for label "L1" on properties "p1", "p2" and "p3"<br>
 * - one for label "L2" on properties "p1", "p2" and "p3"<br>
 */
class SingleNodeKeyConstraintListener implements MappingListener {
  private final String targetName;
  private final List<String> labels;
  private final List<NodeKeyConstraint> schema = new ArrayList<>();
  private final List<String> properties = new ArrayList<>();
  private boolean inArray = false;

  public SingleNodeKeyConstraintListener(String targetName, List<String> labels) {
    this.targetName = targetName;
    this.labels = labels;
  }

  @Override
  public void enterArray() {
    inArray = true;
  }

  @Override
  public void exitArray() {
    addConstraints();
  }

  @Override
  public void enterObjectEntry(String field, String property) {
    properties.add(property);
  }

  @Override
  public void exitObject() {
    if (!inArray) {
      addConstraints();
    }
  }

  @Override
  public void enterString(String value) {
    properties.add(value);
  }

  @Override
  public void exitString() {
    if (!inArray) {
      addConstraints();
    }
  }

  public List<NodeKeyConstraint> getSchema() {
    return schema;
  }

  private void addConstraints() {
    labels.forEach(
        label -> {
          String name =
              String.format(
                  "%s-%s-node-single-key-for-%s", targetName, label, String.join("-", properties));
          schema.add(new NodeKeyConstraint(name, label, new ArrayList<>(properties), null));
        });
    properties.clear();
  }
}

/**
 * SingleRelationshipKeyConstraintListener groups mapping elements into a single key constraint
 * definition. For instance, let us assume the spec defines a relationship target with type "T".
 * That target defines a key mapping as an array of 3 strings (for properties "p1", "p2" and "p3").
 * This listener will generate 1 constraint definition for type "T" on properties "p1", "p2" and
 * "p3".
 */
class SingleRelationshipKeyConstraintListener implements MappingListener {
  private final String targetName;
  private final String type;
  private RelationshipKeyConstraint schema;
  private final List<String> properties = new ArrayList<>();
  private boolean inArray = false;

  public SingleRelationshipKeyConstraintListener(String targetName, String type) {
    this.targetName = targetName;
    this.type = type;
  }

  @Override
  public void enterArray() {
    inArray = true;
  }

  @Override
  public void exitArray() {
    addConstraints();
  }

  @Override
  public void enterObjectEntry(String field, String property) {
    properties.add(property);
  }

  @Override
  public void exitObject() {
    if (!inArray) {
      addConstraints();
    }
  }

  @Override
  public void enterString(String value) {
    properties.add(value);
  }

  @Override
  public void exitString() {
    if (!inArray) {
      addConstraints();
    }
  }

  public RelationshipKeyConstraint getSchema() {
    return schema;
  }

  private void addConstraints() {
    String name =
        String.format(
            "%s-%s-relationship-single-key-for-%s", targetName, type, String.join("-", properties));
    schema = new RelationshipKeyConstraint(name, new ArrayList<>(properties), null);
    properties.clear();
  }
}

/**
 * This listener base implementation groups properties for node constraints as follows: <br>
 * - a JSONArray mapping produces as many node constraints as it contains values (per node target
 * label) <br>
 * - a JSONObject mapping produces a single node constraint (per node target label) with all the
 * properties defined as values <br>
 * - a string mapping produces a single-property single node constraint (per node target label)
 * Because of the legacy spec ambiguity, there as many created constraints as node target labels.
 */
abstract class CompoundNodeSchemaListener<T> implements MappingListener {

  protected final String targetName;
  protected final List<String> labels;
  protected final List<T> schema = new ArrayList<>();
  protected final List<String> properties = new ArrayList<>();

  public CompoundNodeSchemaListener(String targetName, List<String> labels) {
    this.targetName = targetName;
    this.labels = labels;
  }

  @Override
  public void enterObject() {
    properties.clear();
  }

  @Override
  public void enterObjectEntry(String field, String property) {
    properties.add(property);
  }

  @Override
  public void exitObject() {
    labels.forEach(label -> schema.add(newSchemaElement(label, new ArrayList<>(properties))));
    properties.clear();
  }

  @Override
  public void enterString(String property) {
    labels.forEach(
        label -> {
          List<String> properties = List.of(property);
          schema.add(newSchemaElement(label, properties));
        });
  }

  protected abstract T newSchemaElement(String label, List<String> properties);

  public List<T> getSchema() {
    return schema;
  }
}

/**
 * This listener base implementation groups properties for node constraints as follows. <br>
 * - a JSONArray mapping produces as many node constraints as it contains values (per node target
 * label) <br>
 * - a JSONObject mapping produces a single node constraint (per node target label) with all the
 * properties defined as values <br>
 * - a string mapping produces a single-property single node constraint (per node target label)
 * Because of the legacy spec ambiguity, there as many created constraints as node target labels
 */
abstract class CompoundRelationshipSchemaListener<T> implements MappingListener {

  protected final String targetName;
  protected final String type;
  protected final List<T> schema = new ArrayList<>();
  protected final List<String> properties = new ArrayList<>();

  public CompoundRelationshipSchemaListener(String targetName, String type) {
    this.targetName = targetName;
    this.type = type;
  }

  @Override
  public void enterObject() {
    properties.clear();
  }

  @Override
  public void enterObjectEntry(String field, String property) {
    properties.add(property);
  }

  @Override
  public void exitObject() {
    schema.add(newSchemaElement(new ArrayList<>(properties)));
    properties.clear();
  }

  @Override
  public void enterString(String property) {
    schema.add(newSchemaElement(List.of(property)));
  }

  protected abstract T newSchemaElement(List<String> properties);

  public List<T> getSchema() {
    return schema;
  }
}

class NodeKeyConstraintsListener extends CompoundNodeSchemaListener<NodeKeyConstraint> {

  public NodeKeyConstraintsListener(String targetName, List<String> labels) {
    super(targetName, labels);
  }

  @Override
  protected NodeKeyConstraint newSchemaElement(String label, List<String> properties) {
    String name =
        String.format("%s-%s-node-key-for-%s", targetName, label, String.join("-", properties));
    return new NodeKeyConstraint(name, label, properties, null);
  }
}

class RelationshipKeyConstraintsListener
    extends CompoundRelationshipSchemaListener<RelationshipKeyConstraint> {

  public RelationshipKeyConstraintsListener(String targetName, String type) {
    super(targetName, type);
  }

  @Override
  protected RelationshipKeyConstraint newSchemaElement(List<String> properties) {
    String name =
        String.format(
            "%s-%s-relationship-key-for-%s", targetName, type, String.join("-", properties));
    return new RelationshipKeyConstraint(name, properties, null);
  }
}

class NodeUniqueConstraintsListener extends CompoundNodeSchemaListener<NodeUniqueConstraint> {

  public NodeUniqueConstraintsListener(String targetName, List<String> labels) {
    super(targetName, labels);
  }

  @Override
  protected NodeUniqueConstraint newSchemaElement(String label, List<String> properties) {
    String name =
        String.format("%s-%s-node-unique-for-%s", targetName, label, String.join("-", properties));
    return new NodeUniqueConstraint(name, label, properties, null);
  }
}

class RelationshipUniqueConstraintsListener
    extends CompoundRelationshipSchemaListener<RelationshipUniqueConstraint> {

  public RelationshipUniqueConstraintsListener(String targetName, String type) {
    super(targetName, type);
  }

  @Override
  protected RelationshipUniqueConstraint newSchemaElement(List<String> properties) {
    String name =
        String.format(
            "%s-%s-relationship-unique-for-%s", targetName, type, String.join("-", properties));
    return new RelationshipUniqueConstraint(name, properties, null);
  }
}

class NodeExistenceConstraintListener implements MappingListener {

  private final String targetName;
  private final List<String> labels;
  private final List<NodeExistenceConstraint> schema = new ArrayList<>();

  public NodeExistenceConstraintListener(String targetName, List<String> labels) {
    this.targetName = targetName;
    this.labels = labels;
  }

  @Override
  public void enterObjectEntry(String field, String property) {
    addConstraint(property);
  }

  @Override
  public void enterString(String property) {
    addConstraint(property);
  }

  public List<NodeExistenceConstraint> getSchema() {
    return schema;
  }

  private void addConstraint(String property) {
    labels.forEach(
        (label) -> {
          String name = String.format("%s-%s-node-not-null-for-%s", targetName, label, property);
          schema.add(new NodeExistenceConstraint(name, label, property));
        });
  }
}

class RelationshipExistenceConstraintListener implements MappingListener {

  private final String targetName;
  private final String type;
  private final List<RelationshipExistenceConstraint> schema = new ArrayList<>();

  public RelationshipExistenceConstraintListener(String targetName, String type) {
    this.targetName = targetName;
    this.type = type;
  }

  @Override
  public void enterObjectEntry(String field, String property) {
    addConstraint(property);
  }

  @Override
  public void enterString(String property) {
    addConstraint(property);
  }

  public List<RelationshipExistenceConstraint> getSchema() {
    return schema;
  }

  private void addConstraint(String property) {
    String name = String.format("%s-%s-relationship-not-null-for-%s", targetName, type, property);
    schema.add(new RelationshipExistenceConstraint(name, property));
  }
}

class NodeIndexListener extends CompoundNodeSchemaListener<NodeRangeIndex> {

  public NodeIndexListener(String targetName, List<String> labels) {
    super(targetName, labels);
  }

  @Override
  protected NodeRangeIndex newSchemaElement(String label, List<String> properties) {
    String name =
        String.format(
            "%s-%s-node-range-index-for-%s", targetName, label, String.join("-", properties));
    return new NodeRangeIndex(name, label, properties);
  }
}

class RelationshipIndexListener extends CompoundRelationshipSchemaListener<RelationshipRangeIndex> {

  public RelationshipIndexListener(String targetName, String type) {
    super(targetName, type);
  }

  @Override
  protected RelationshipRangeIndex newSchemaElement(List<String> properties) {
    String name =
        String.format(
            "%s-%s-relationship-range-index-for-%s",
            targetName, type, String.join("-", properties));
    return new RelationshipRangeIndex(name, properties);
  }
}
