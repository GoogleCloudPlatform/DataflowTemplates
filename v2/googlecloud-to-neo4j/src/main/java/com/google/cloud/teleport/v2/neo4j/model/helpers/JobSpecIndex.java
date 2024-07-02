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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import org.neo4j.importer.v1.actions.ActionStage;

class JobSpecIndex {

  private final Set<String> nodeTargets = new LinkedHashSet<>();
  private final Set<String> edgeTargets = new LinkedHashSet<>();
  private final Set<String> customQueryTargets = new LinkedHashSet<>();
  private final Map<String, ActionStage> actionStages = new HashMap<>();

  private final Map<String, Dependency> dependencyGraph = new LinkedHashMap<>();

  public void trackNode(String name, String executeAfter, String executeAfterName) {
    nodeTargets.add(name);
    if (executeAfter.isEmpty()) {
      return;
    }
    if (!dependsOnAction(executeAfter)) {
      registerDependency(name, executeAfter, executeAfterName);
      return;
    }
    var stage = actionStages.get(executeAfterName);
    if (stage == null) {
      actionStages.put(executeAfterName, ActionStage.PRE_NODES);
      return;
    }
    switch (stage) {
      case START:
      case PRE_NODES:
        return;
      case PRE_RELATIONSHIPS:
      case PRE_QUERIES:
        actionStages.put(executeAfterName, ActionStage.START);
        break;
      default:
        throw incompatibleStageException(executeAfterName, stage, ActionStage.PRE_NODES);
    }
  }

  public void trackEdge(String name, String executeAfter, String executeAfterName) {
    edgeTargets.add(name);
    if (executeAfter.isEmpty()) {
      return;
    }
    if (!dependsOnAction(executeAfter)) {
      registerDependency(name, executeAfter, executeAfterName);
      return;
    }
    var stage = actionStages.get(executeAfterName);
    if (stage == null) {
      actionStages.put(executeAfterName, ActionStage.PRE_RELATIONSHIPS);
      return;
    }
    switch (stage) {
      case START:
      case PRE_RELATIONSHIPS:
        return;
      case PRE_NODES:
      case PRE_QUERIES:
        actionStages.put(executeAfterName, ActionStage.START);
        break;
      default:
        throw incompatibleStageException(executeAfterName, stage, ActionStage.PRE_RELATIONSHIPS);
    }
  }

  public void trackCustomQuery(String name, String executeAfter, String executeAfterName) {
    customQueryTargets.add(name);
    if (executeAfter.isEmpty()) {
      return;
    }
    if (!dependsOnAction(executeAfter)) {
      registerDependency(name, executeAfter, executeAfterName);
      return;
    }
    var stage = actionStages.get(executeAfterName);
    if (stage == null) {
      actionStages.put(executeAfterName, ActionStage.PRE_QUERIES);
      return;
    }
    switch (stage) {
      case START:
      case PRE_QUERIES:
        return;
      case PRE_NODES:
      case PRE_RELATIONSHIPS:
        actionStages.put(executeAfterName, ActionStage.START);
        break;
      default:
        throw incompatibleStageException(executeAfterName, stage, ActionStage.PRE_QUERIES);
    }
  }

  public void trackAction(String name, String executeAfter) {
    if (executeAfter.isEmpty()) {
      return;
    }
    var previousStage = actionStages.get(name);
    if (previousStage == ActionStage.END) {
      return;
    }
    switch (executeAfter.toLowerCase(Locale.ROOT)) {
      case "sources":
      case "source":
        throwIfDependedUpon(name, previousStage, ActionStage.POST_SOURCES);
        actionStages.put(name, ActionStage.POST_SOURCES);
        break;
      case "nodes":
      case "node":
        throwIfDependedUpon(name, previousStage, ActionStage.POST_NODES);
        actionStages.put(name, ActionStage.POST_NODES);
        break;
      case "edges":
      case "edge":
        throwIfDependedUpon(name, previousStage, ActionStage.POST_RELATIONSHIPS);
        actionStages.put(name, ActionStage.POST_RELATIONSHIPS);
        break;
      case "custom_queries":
      case "custom_query":
        throwIfDependedUpon(name, previousStage, ActionStage.POST_QUERIES);
        actionStages.put(name, ActionStage.POST_QUERIES);
        break;
      case "action":
      case "loads":
        throwIfDependedUpon(name, previousStage, ActionStage.END);
        actionStages.put(name, ActionStage.END);
        break;
      case "start":
      case "async":
      case "preloads":
        actionStages.put(name, ActionStage.START);
        break;
    }
  }

  public List<String> getDependencies(String name) {
    var result = new ArrayList<String>();
    var visited = new HashSet<String>();
    visited.add(name);
    var stack = new Stack<String>();
    stack.addAll(directDependenciesOf(name));
    while (!stack.isEmpty()) {
      var dependency = stack.pop();
      if (!visited.contains(dependency)) {
        result.add(dependency);
        visited.add(dependency);
        stack.addAll(directDependenciesOf(dependency));
      }
    }
    return result;
  }

  public ActionStage getActionStage(String name) {
    return actionStages.getOrDefault(name, ActionStage.END);
  }

  private void registerDependency(String name, String executeAfter, String executeAfterName) {
    String dependencyType = executeAfter.toLowerCase(Locale.ROOT);
    if (!executeAfterName.isEmpty()) {
      dependencyGraph.put(name, new NamedDependency(dependencyType, executeAfterName));
      return;
    }
    dependencyGraph.put(name, new DependencyGroup(dependencyType));
  }

  private Collection<String> directDependenciesOf(String name) {
    if (!dependencyGraph.containsKey(name)) {
      return implicitDirectDependenciesOf(name);
    }
    var dependency = dependencyGraph.get(name);
    if (dependency instanceof NamedDependency) {
      String dependencyName = ((NamedDependency) dependency).getName();
      return List.of(dependencyName);
    }
    var group = (DependencyGroup) dependency;
    String groupType = group.getType();
    switch (groupType) {
      case "nodes":
        return nodeTargets;
      case "edges":
        return edgeTargets;
      case "custom_queries":
        return customQueryTargets;
      default:
        throw new RuntimeException(
            String.format("Unknown dependency (execute_after) type: %s", groupType));
    }
  }

  private List<String> implicitDirectDependenciesOf(String name) {
    var result = new LinkedHashSet<String>();
    if (edgeTargets.contains(name) || customQueryTargets.contains(name)) {
      result.addAll(nodeTargets);
    }
    if (customQueryTargets.contains(name)) {
      result.addAll(edgeTargets);
    }
    return new ArrayList<>(result);
  }

  private static void throwIfDependedUpon(
      String name, ActionStage previousStage, ActionStage newStage) {
    // START is included here as it can mean several targets of different types depend on the same
    // action
    // see trackNode, trackEdge, trackCustomQuery
    if (previousStage == ActionStage.START
        || previousStage == ActionStage.PRE_NODES
        || previousStage == ActionStage.PRE_RELATIONSHIPS
        || previousStage == ActionStage.PRE_QUERIES) {
      throw incompatibleStageException(name, previousStage, newStage);
    }
  }

  private static IllegalArgumentException incompatibleStageException(
      String actionName, ActionStage previousStage, ActionStage newStage) {
    return new IllegalArgumentException(
        String.format(
            "Cannot reconcile stage for action \"%s\": it was initially %s and needs to be %s",
            actionName, previousStage, newStage));
  }

  private static boolean dependsOnAction(String executeAfter) {
    return executeAfter.toLowerCase(Locale.ROOT).equals("action");
  }
}

interface Dependency {}

class NamedDependency implements Dependency {
  private final String type;
  private final String name;

  public NamedDependency(String type, String name) {
    this.type = type;
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof NamedDependency)) {
      return false;
    }

    NamedDependency that = (NamedDependency) o;
    return Objects.equals(type, that.type) && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, name);
  }
}

class DependencyGroup implements Dependency {
  private final String type;

  public DependencyGroup(String type) {
    this.type = type;
  }

  public String getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof DependencyGroup)) {
      return false;
    }

    DependencyGroup that = (DependencyGroup) o;
    return Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(type);
  }
}
