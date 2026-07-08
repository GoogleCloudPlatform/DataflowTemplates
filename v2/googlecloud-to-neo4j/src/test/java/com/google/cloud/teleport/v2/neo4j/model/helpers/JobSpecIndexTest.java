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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.List;
import org.junit.Test;
import org.neo4j.importer.v1.actions.ActionStage;

public class JobSpecIndexTest {
  private final JobSpecIndex index = new JobSpecIndex();

  @Test
  public void resolves_node_named_explicit_dependency() {
    index.trackNode("a-node", "edge", "an-edge");
    index.trackEdge("an-edge", "", "");

    var dependencies = index.getDependencies("a-node");

    assertThat(dependencies).containsExactlyElementsIn(List.of("an-edge"));
  }

  @Test
  public void transitively_resolves_node_explicit_dependencies() {
    index.trackNode("a-node", "edge", "an-edge");
    index.trackEdge("an-edge", "custom_query", "a-query");

    var dependencies = index.getDependencies("a-node");

    assertThat(dependencies).containsExactlyElementsIn(List.of("an-edge", "a-query"));
  }

  @Test
  public void resolves_node_explicit_dependency_group() {
    index.trackCustomQuery("a-query-1", "", "");
    index.trackNode("a-node", "custom_queries", "");
    index.trackCustomQuery("a-query-2", "", "");

    var dependencies = index.getDependencies("a-node");

    assertThat(dependencies).containsExactlyElementsIn(List.of("a-query-1", "a-query-2"));
  }

  @Test
  public void transitively_resolves_node_dependency_group() {
    index.trackCustomQuery("a-query", "edges", "");
    index.trackEdge("an-edge-1", "", "");
    index.trackNode("a-node", "custom_query", "a-query");
    index.trackEdge("an-edge-2", "", "");

    var dependencies = index.getDependencies("a-node");

    assertThat(dependencies)
        .containsExactlyElementsIn(List.of("a-query", "an-edge-1", "an-edge-2"));
  }

  @Test
  public void resolves_node_peer_dependencies_excluding_itself() {
    index.trackNode("a-node-1", "", "");
    index.trackNode("a-node-2", "nodes", "");
    index.trackNode("a-node-3", "", "");

    var dependencies = index.getDependencies("a-node-2");

    assertThat(dependencies).containsExactlyElementsIn(List.of("a-node-1", "a-node-3"));
  }

  @Test
  public void transitively_resolves_node_peer_dependencies_excluding_itself() {
    index.trackCustomQuery("a-query", "edges", "");
    index.trackNode("a-node-1", "", "");
    index.trackNode("a-node-2", "custom_query", "a-query");
    index.trackNode("a-node-3", "", "");
    index.trackEdge("an-edge-1", "nodes", "");
    index.trackEdge("an-edge-2", "custom_queries", "");

    var dependencies = index.getDependencies("a-node-2");

    assertThat(dependencies)
        .containsExactlyElementsIn(
            List.of("a-query", "an-edge-1", "an-edge-2", "a-node-1", "a-node-3"));
  }

  @Test
  public void resolves_cycles() {
    index.trackNode("a-node", "edge", "an-edge");
    index.trackEdge("an-edge", "node", "a-node");

    assertThat(index.getDependencies("a-node")).containsExactlyElementsIn(List.of("an-edge"));
    assertThat(index.getDependencies("an-edge")).containsExactlyElementsIn(List.of("a-node"));
  }

  @Test
  public void resolves_implicit_node_dependencies_of_dependencyless_edge() {
    index.trackNode("a-node-1", "", "");
    index.trackNode("a-node-2", "", "");
    index.trackEdge("an-edge", "", "");
    index.trackCustomQuery("a-query", "", "");

    var dependencies = index.getDependencies("an-edge");

    assertThat(dependencies).containsExactlyElementsIn(List.of("a-node-1", "a-node-2"));
  }

  @Test
  public void transitively_resolves_implicit_node_dependencies_of_dependencyless_edge() {
    index.trackNode("a-node-1", "custom_query", "a-query-1");
    index.trackNode("a-node-2", "", "");
    index.trackEdge("an-edge", "", "");
    index.trackCustomQuery("a-query-1", "custom_queries", "");
    index.trackCustomQuery("a-query-2", "", "");

    var dependencies = index.getDependencies("an-edge");

    assertThat(dependencies)
        .containsExactlyElementsIn(List.of("a-node-1", "a-node-2", "a-query-1", "a-query-2"));
  }

  @Test
  public void resolves_implicit_node_and_edge_dependencies_of_dependencyless_query() {
    index.trackNode("a-node-1", "", "");
    index.trackNode("a-node-2", "", "");
    index.trackEdge("an-edge", "", "");
    index.trackCustomQuery("a-query", "", "");

    var dependencies = index.getDependencies("an-edge");

    assertThat(dependencies).containsExactlyElementsIn(List.of("a-node-1", "a-node-2"));
  }

  @Test
  public void transitively_resolves_implicit_node_and_edge_dependencies_of_dependencyless_query() {
    index.trackNode("a-node-1", "custom_queries", "");
    index.trackNode("a-node-2", "", "");
    index.trackEdge("an-edge", "custom_query", "a-query-3");
    index.trackCustomQuery("a-query-1", "", "");
    index.trackCustomQuery("a-query-2", "", "");
    index.trackCustomQuery("a-query-3", "", "");

    var dependencies = index.getDependencies("a-query-1");

    assertThat(dependencies)
        .containsExactlyElementsIn(
            List.of("a-node-1", "a-node-2", "an-edge", "a-query-2", "a-query-3"));
  }

  @Test
  public void resolves_default_action_stage() {
    index.trackAction("an-action", "");

    var stage = index.getActionStage("an-action");

    assertThat(stage).isEqualTo(ActionStage.END);
  }

  @Test
  public void resolves_action_stage_to_start() {
    index.trackAction("an-action-1", "start");
    index.trackAction("an-action-2", "async");
    index.trackAction("an-action-3", "preloads");

    assertThat(index.getActionStage("an-action-1")).isEqualTo(ActionStage.START);
    assertThat(index.getActionStage("an-action-2")).isEqualTo(ActionStage.START);
    assertThat(index.getActionStage("an-action-3")).isEqualTo(ActionStage.START);
  }

  @Test
  public void resolves_action_stage_from_sources_dependency() {
    index.trackAction("an-action", "sources");

    var stage = index.getActionStage("an-action");

    assertThat(stage).isEqualTo(ActionStage.POST_SOURCES);
  }

  @Test
  public void resolves_action_stage_from_source_dependency() {
    index.trackAction("an-action", "source");

    var stage = index.getActionStage("an-action");

    assertThat(stage).isEqualTo(ActionStage.POST_SOURCES);
  }

  @Test
  public void resolves_action_stage_from_nodes_dependency() {
    index.trackAction("an-action", "nodes");

    var stage = index.getActionStage("an-action");

    assertThat(stage).isEqualTo(ActionStage.POST_NODES);
  }

  @Test
  public void resolves_action_stage_from_a_node_dependency() {
    index.trackAction("an-action", "nodes");

    var stage = index.getActionStage("an-action");

    assertThat(stage).isEqualTo(ActionStage.POST_NODES);
  }

  @Test
  public void resolves_action_stage_from_edges_dependency() {
    index.trackAction("an-action", "edges");

    var stage = index.getActionStage("an-action");

    assertThat(stage).isEqualTo(ActionStage.POST_RELATIONSHIPS);
  }

  @Test
  public void resolves_action_stage_from_an_edge_dependency() {
    index.trackAction("an-action", "edge");

    var stage = index.getActionStage("an-action");

    assertThat(stage).isEqualTo(ActionStage.POST_RELATIONSHIPS);
  }

  @Test
  public void resolves_action_stage_from_queries_dependency() {
    index.trackAction("an-action", "custom_queries");

    var stage = index.getActionStage("an-action");

    assertThat(stage).isEqualTo(ActionStage.POST_QUERIES);
  }

  @Test
  public void resolves_action_stage_from_a_query_dependency() {
    index.trackAction("an-action", "custom_query");

    var stage = index.getActionStage("an-action");

    assertThat(stage).isEqualTo(ActionStage.POST_QUERIES);
  }

  @Test
  public void resolves_action_stage_from_an_action_dependency() {
    index.trackAction("an-action", "action");

    var stage = index.getActionStage("an-action");

    assertThat(stage).isEqualTo(ActionStage.END);
  }

  @Test
  public void resolves_action_stage_from_loads_dependency() {
    index.trackAction("an-action", "loads");

    var stage = index.getActionStage("an-action");

    assertThat(stage).isEqualTo(ActionStage.END);
  }

  @Test
  public void resolves_action_stage_from_node_dependency() {
    index.trackNode("a-node", "action", "an-action");
    index.trackAction("an-action", "");

    var stage = index.getActionStage("an-action");

    assertThat(stage).isEqualTo(ActionStage.PRE_NODES);
  }

  @Test
  public void resolves_action_stage_from_edge_dependency() {
    index.trackEdge("an-edge", "action", "an-action");
    index.trackAction("an-action", "");

    var stage = index.getActionStage("an-action");

    assertThat(stage).isEqualTo(ActionStage.PRE_RELATIONSHIPS);
  }

  @Test
  public void resolves_action_stage_from_query_dependency() {
    index.trackCustomQuery("a-query", "action", "an-action");
    index.trackAction("an-action", "");

    var stage = index.getActionStage("an-action");

    assertThat(stage).isEqualTo(ActionStage.PRE_QUERIES);
  }

  @Test
  public void resolves_earliest_action_stage_from_multiple_dependencies() {
    index.trackNode("a-node-1", "action", "an-action-1");
    index.trackAction("an-action-1", "");
    index.trackEdge("an-edge-1", "action", "an-action-1");
    assertThat(index.getActionStage("an-action-1")).isEqualTo(ActionStage.START);
    index.trackNode("a-node-2", "action", "an-action-2");
    index.trackAction("an-action-2", "");
    index.trackCustomQuery("a-query-2", "action", "an-action-2");
    assertThat(index.getActionStage("an-action-2")).isEqualTo(ActionStage.START);
    index.trackEdge("an-edge-3", "action", "an-action-3");
    index.trackAction("an-action-3", "");
    index.trackCustomQuery("a-query-3", "action", "an-action-3");
    assertThat(index.getActionStage("an-action-3")).isEqualTo(ActionStage.START);
    index.trackEdge("an-edge-4", "action", "an-action-4");
    index.trackAction("an-action-4", "");
    index.trackNode("a-query-4", "action", "an-action-4");
    assertThat(index.getActionStage("an-action-3")).isEqualTo(ActionStage.START);
  }

  @Test
  public void keeps_start_stage_with_node_dependent() {
    index.trackAction("an-action", "start");
    index.trackNode("a-node", "action", "an-action");

    assertThat(index.getActionStage("an-action")).isEqualTo(ActionStage.START);
  }

  @Test
  public void overrides_to_start_stage_after_node_dependent() {
    index.trackNode("a-node", "action", "an-action");
    index.trackAction("an-action", "start");

    assertThat(index.getActionStage("an-action")).isEqualTo(ActionStage.START);
  }

  @Test
  public void keeps_start_stage_with_edge_dependent() {
    index.trackAction("an-action", "start");
    index.trackEdge("an-edge", "action", "an-action");

    assertThat(index.getActionStage("an-action")).isEqualTo(ActionStage.START);
  }

  @Test
  public void overrides_to_start_stage_after_edge_dependent() {
    index.trackEdge("an-edge", "action", "an-action");
    index.trackAction("an-action", "start");

    assertThat(index.getActionStage("an-action")).isEqualTo(ActionStage.START);
  }

  @Test
  public void keeps_start_stage_with_query_dependent() {
    index.trackAction("an-action", "start");
    index.trackCustomQuery("a-query", "action", "an-action");

    assertThat(index.getActionStage("an-action")).isEqualTo(ActionStage.START);
  }

  @Test
  public void overrides_to_start_stage_after_query_dependent() {
    index.trackCustomQuery("a-query", "action", "an-action");
    index.trackAction("an-action", "start");

    assertThat(index.getActionStage("an-action")).isEqualTo(ActionStage.START);
  }

  @Test
  public void resolves_same_stage_for_multiple_node_dependents() {
    index.trackNode("a-node-1", "action", "an-action");
    index.trackNode("a-node-2", "action", "an-action");

    assertThat(index.getActionStage("an-action")).isEqualTo(ActionStage.PRE_NODES);
  }

  @Test
  public void resolves_same_stage_for_multiple_edge_dependents() {
    index.trackEdge("an-edge-1", "action", "an-action");
    index.trackEdge("an-edge-2", "action", "an-action");

    assertThat(index.getActionStage("an-action")).isEqualTo(ActionStage.PRE_RELATIONSHIPS);
  }

  @Test
  public void resolves_same_stage_for_multiple_query_dependents() {
    index.trackCustomQuery("a-query-1", "action", "an-action");
    index.trackCustomQuery("a-query-2", "action", "an-action");

    assertThat(index.getActionStage("an-action")).isEqualTo(ActionStage.PRE_QUERIES);
  }

  @Test
  public void rejects_incompatible_sources_dependency_and_node_dependent() {
    index.trackAction("an-action", "sources");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackNode("a-node", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_SOURCES and needs to be PRE_NODES");
  }

  @Test
  public void rejects_incompatible_node_dependent_and_sources_dependency() {
    index.trackNode("a-node", "action", "an-action");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackAction("an-action", "sources"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_NODES and needs to be POST_SOURCES");
  }

  @Test
  public void rejects_incompatible_sources_dependency_and_edge_dependent() {
    index.trackAction("an-action", "sources");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackEdge("an-edge", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_SOURCES and needs to be PRE_RELATIONSHIPS");
  }

  @Test
  public void rejects_incompatible_edge_dependent_and_sources_dependency() {
    index.trackEdge("an-edge", "action", "an-action");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackAction("an-action", "sources"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_RELATIONSHIPS and needs to be POST_SOURCES");
  }

  @Test
  public void rejects_incompatible_sources_dependency_and_query_dependent() {
    index.trackAction("an-action", "sources");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackCustomQuery("a-custom-query", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_SOURCES and needs to be PRE_QUERIES");
  }

  @Test
  public void rejects_incompatible_query_dependent_and_sources_dependency() {
    index.trackCustomQuery("a-custom-query", "action", "an-action");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackAction("an-action", "sources"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_QUERIES and needs to be POST_SOURCES");
  }

  @Test
  public void rejects_incompatible_source_dependency_and_node_dependent() {
    index.trackAction("an-action", "source");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackNode("a-node", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_SOURCES and needs to be PRE_NODES");
  }

  @Test
  public void rejects_incompatible_node_dependent_and_source_dependency() {
    index.trackNode("a-node", "action", "an-action");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackAction("an-action", "source"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_NODES and needs to be POST_SOURCES");
  }

  @Test
  public void rejects_incompatible_source_dependency_and_edge_dependent() {
    index.trackAction("an-action", "source");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackEdge("an-edge", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_SOURCES and needs to be PRE_RELATIONSHIPS");
  }

  @Test
  public void rejects_incompatible_edge_dependent_and_source_dependency() {
    index.trackEdge("an-edge", "action", "an-action");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackAction("an-action", "source"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_RELATIONSHIPS and needs to be POST_SOURCES");
  }

  @Test
  public void rejects_incompatible_source_dependency_and_query_dependent() {
    index.trackAction("an-action", "source");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackCustomQuery("a-query", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_SOURCES and needs to be PRE_QUERIES");
  }

  @Test
  public void rejects_incompatible_query_dependent_and_source_dependency() {
    index.trackCustomQuery("a-query", "action", "an-action");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackAction("an-action", "source"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_QUERIES and needs to be POST_SOURCES");
  }

  @Test
  public void rejects_incompatible_nodes_dependency_and_node_dependent() {
    index.trackAction("an-action", "nodes");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackNode("a-node", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_NODES and needs to be PRE_NODES");
  }

  @Test
  public void rejects_incompatible_node_dependent_and_nodes_dependency() {
    index.trackNode("a-node", "action", "an-action");

    var exception =
        assertThrows(IllegalArgumentException.class, () -> index.trackAction("an-action", "nodes"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_NODES and needs to be POST_NODES");
  }

  @Test
  public void rejects_incompatible_nodes_dependency_and_edge_dependent() {
    index.trackAction("an-action", "nodes");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackEdge("an-edge", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_NODES and needs to be PRE_RELATIONSHIPS");
  }

  @Test
  public void rejects_incompatible_edge_dependent_and_nodes_dependency() {
    index.trackEdge("an-edge", "action", "an-action");

    var exception =
        assertThrows(IllegalArgumentException.class, () -> index.trackAction("an-action", "nodes"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_RELATIONSHIPS and needs to be POST_NODES");
  }

  @Test
  public void rejects_incompatible_nodes_dependency_and_query_dependent() {
    index.trackAction("an-action", "nodes");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackCustomQuery("a-custom-query", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_NODES and needs to be PRE_QUERIES");
  }

  @Test
  public void rejects_incompatible_query_dependent_and_nodes_dependency() {
    index.trackCustomQuery("a-custom-query", "action", "an-action");

    var exception =
        assertThrows(IllegalArgumentException.class, () -> index.trackAction("an-action", "nodes"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_QUERIES and needs to be POST_NODES");
  }

  @Test
  public void rejects_incompatible_node_dependency_and_node_dependent() {
    index.trackAction("an-action", "node");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackNode("a-node", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_NODES and needs to be PRE_NODES");
  }

  @Test
  public void rejects_incompatible_node_dependent_and_node_dependency() {
    index.trackNode("a-node", "action", "an-action");

    var exception =
        assertThrows(IllegalArgumentException.class, () -> index.trackAction("an-action", "node"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_NODES and needs to be POST_NODES");
  }

  @Test
  public void rejects_incompatible_node_dependency_and_edge_dependent() {
    index.trackAction("an-action", "node");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackEdge("an-edge", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_NODES and needs to be PRE_RELATIONSHIPS");
  }

  @Test
  public void rejects_incompatible_edge_dependent_and_node_dependency() {
    index.trackEdge("an-edge", "action", "an-action");

    var exception =
        assertThrows(IllegalArgumentException.class, () -> index.trackAction("an-action", "node"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_RELATIONSHIPS and needs to be POST_NODES");
  }

  @Test
  public void rejects_incompatible_node_dependency_and_query_dependent() {
    index.trackAction("an-action", "node");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackCustomQuery("a-custom-query", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_NODES and needs to be PRE_QUERIES");
  }

  @Test
  public void rejects_incompatible_query_dependent_and_node_dependency() {
    index.trackCustomQuery("a-custom-query", "action", "an-action");

    var exception =
        assertThrows(IllegalArgumentException.class, () -> index.trackAction("an-action", "node"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_QUERIES and needs to be POST_NODES");
  }

  @Test
  public void rejects_incompatible_edges_dependency_and_node_dependent() {
    index.trackAction("an-action", "edges");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackNode("a-node", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_RELATIONSHIPS and needs to be PRE_NODES");
  }

  @Test
  public void rejects_incompatible_node_dependent_and_edges_dependency() {
    index.trackNode("a-node", "action", "an-action");

    var exception =
        assertThrows(IllegalArgumentException.class, () -> index.trackAction("an-action", "edges"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_NODES and needs to be POST_RELATIONSHIPS");
  }

  @Test
  public void rejects_incompatible_edges_dependency_and_edge_dependent() {
    index.trackAction("an-action", "edges");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackEdge("an-edge", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_RELATIONSHIPS and needs to be PRE_RELATIONSHIPS");
  }

  @Test
  public void rejects_incompatible_edge_dependent_and_edges_dependency() {
    index.trackEdge("an-edge", "action", "an-action");

    var exception =
        assertThrows(IllegalArgumentException.class, () -> index.trackAction("an-action", "edges"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_RELATIONSHIPS and needs to be POST_RELATIONSHIPS");
  }

  @Test
  public void rejects_incompatible_edges_dependency_and_query_dependent() {
    index.trackAction("an-action", "edges");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackCustomQuery("a-custom-query", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_RELATIONSHIPS and needs to be PRE_QUERIES");
  }

  @Test
  public void rejects_incompatible_query_dependent_and_edges_dependency() {
    index.trackCustomQuery("a-custom-query", "action", "an-action");

    var exception =
        assertThrows(IllegalArgumentException.class, () -> index.trackAction("an-action", "edges"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_QUERIES and needs to be POST_RELATIONSHIPS");
  }

  @Test
  public void rejects_incompatible_edge_dependency_and_node_dependent() {
    index.trackAction("an-action", "edge");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackNode("a-node", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_RELATIONSHIPS and needs to be PRE_NODES");
  }

  @Test
  public void rejects_incompatible_node_dependent_and_edge_dependency() {
    index.trackNode("a-node", "action", "an-action");

    var exception =
        assertThrows(IllegalArgumentException.class, () -> index.trackAction("an-action", "edge"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_NODES and needs to be POST_RELATIONSHIPS");
  }

  @Test
  public void rejects_incompatible_edge_dependency_and_edge_dependent() {
    index.trackAction("an-action", "edge");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackEdge("an-edge", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_RELATIONSHIPS and needs to be PRE_RELATIONSHIPS");
  }

  @Test
  public void rejects_incompatible_edge_dependent_and_edge_dependency() {
    index.trackEdge("an-edge", "action", "an-action");

    var exception =
        assertThrows(IllegalArgumentException.class, () -> index.trackAction("an-action", "edge"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_RELATIONSHIPS and needs to be POST_RELATIONSHIPS");
  }

  @Test
  public void rejects_incompatible_edge_dependency_and_query_dependent() {
    index.trackAction("an-action", "edge");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackCustomQuery("a-custom-query", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_RELATIONSHIPS and needs to be PRE_QUERIES");
  }

  @Test
  public void rejects_incompatible_query_dependent_and_edge_dependency() {
    index.trackCustomQuery("a-custom-query", "action", "an-action");

    var exception =
        assertThrows(IllegalArgumentException.class, () -> index.trackAction("an-action", "edge"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_QUERIES and needs to be POST_RELATIONSHIPS");
  }

  @Test
  public void rejects_incompatible_queries_dependency_and_node_dependent() {
    index.trackAction("an-action", "custom_queries");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackNode("a-node", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_QUERIES and needs to be PRE_NODES");
  }

  @Test
  public void rejects_incompatible_node_dependent_and_queries_dependency() {
    index.trackNode("a-node", "action", "an-action");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackAction("an-action", "custom_queries"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_NODES and needs to be POST_QUERIES");
  }

  @Test
  public void rejects_incompatible_queries_dependency_and_edge_dependent() {
    index.trackAction("an-action", "custom_queries");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackEdge("an-edge", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_QUERIES and needs to be PRE_RELATIONSHIPS");
  }

  @Test
  public void rejects_incompatible_edge_dependent_and_queries_dependency() {
    index.trackEdge("an-edge", "action", "an-action");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackAction("an-action", "custom_queries"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_RELATIONSHIPS and needs to be POST_QUERIES");
  }

  @Test
  public void rejects_incompatible_queries_dependency_and_query_dependent() {
    index.trackAction("an-action", "custom_queries");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackCustomQuery("a-custom-query", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_QUERIES and needs to be PRE_QUERIES");
  }

  @Test
  public void rejects_incompatible_query_dependent_and_queries_dependency() {
    index.trackCustomQuery("a-custom-query", "action", "an-action");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackAction("an-action", "custom_queries"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_QUERIES and needs to be POST_QUERIES");
  }

  @Test
  public void rejects_incompatible_query_dependency_and_node_dependent() {
    index.trackAction("an-action", "custom_query");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackNode("a-node", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_QUERIES and needs to be PRE_NODES");
  }

  @Test
  public void rejects_incompatible_node_dependent_and_query_dependency() {
    index.trackNode("a-node", "action", "an-action");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackAction("an-action", "custom_query"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_NODES and needs to be POST_QUERIES");
  }

  @Test
  public void rejects_incompatible_query_dependency_and_edge_dependent() {
    index.trackAction("an-action", "custom_query");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackEdge("an-edge", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_QUERIES and needs to be PRE_RELATIONSHIPS");
  }

  @Test
  public void rejects_incompatible_edge_dependent_and_query_dependency() {
    index.trackEdge("an-edge", "action", "an-action");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackAction("an-action", "custom_query"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_RELATIONSHIPS and needs to be POST_QUERIES");
  }

  @Test
  public void rejects_incompatible_query_dependency_and_query_dependent() {
    index.trackAction("an-action", "custom_query");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackCustomQuery("a-custom-query", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially POST_QUERIES and needs to be PRE_QUERIES");
  }

  @Test
  public void rejects_incompatible_query_dependent_and_query_dependency() {
    index.trackCustomQuery("a-custom-query", "action", "an-action");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackAction("an-action", "custom_query"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_QUERIES and needs to be POST_QUERIES");
  }

  @Test
  public void rejects_incompatible_action_dependency_and_node_dependent() {
    index.trackAction("an-action", "action");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackNode("a-node", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially END and needs to be PRE_NODES");
  }

  @Test
  public void rejects_incompatible_node_dependent_and_action_dependency() {
    index.trackNode("a-node", "action", "an-action");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackAction("an-action", "action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_NODES and needs to be END");
  }

  @Test
  public void rejects_incompatible_action_dependency_and_edge_dependent() {
    index.trackAction("an-action", "action");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackEdge("an-edge", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially END and needs to be PRE_RELATIONSHIPS");
  }

  @Test
  public void rejects_incompatible_edge_dependent_and_action_dependency() {
    index.trackEdge("an-edge", "action", "an-action");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackAction("an-action", "action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_RELATIONSHIPS and needs to be END");
  }

  @Test
  public void rejects_incompatible_action_dependency_and_query_dependent() {
    index.trackAction("an-action", "action");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackCustomQuery("a-custom-query", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially END and needs to be PRE_QUERIES");
  }

  @Test
  public void rejects_incompatible_query_dependent_and_action_dependency() {
    index.trackCustomQuery("a-custom-query", "action", "an-action");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackAction("an-action", "action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_QUERIES and needs to be END");
  }

  @Test
  public void rejects_incompatible_loads_dependency_and_node_dependent() {
    index.trackAction("an-action", "loads");

    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> index.trackNode("a-node", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially END and needs to be PRE_NODES");
  }

  @Test
  public void rejects_incompatible_node_dependent_and_loads_dependency() {
    index.trackNode("a-node", "action", "an-action");

    var exception =
        assertThrows(IllegalArgumentException.class, () -> index.trackAction("an-action", "loads"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_NODES and needs to be END");
  }

  @Test
  public void rejects_incompatible_loads_dependency_and_edge_dependent() {
    index.trackAction("an-action", "loads");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackEdge("an-edge", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially END and needs to be PRE_RELATIONSHIPS");
  }

  @Test
  public void rejects_incompatible_edge_dependent_and_loads_dependency() {
    index.trackEdge("an-edge", "action", "an-action");

    var exception =
        assertThrows(IllegalArgumentException.class, () -> index.trackAction("an-action", "loads"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_RELATIONSHIPS and needs to be END");
  }

  @Test
  public void rejects_incompatible_loads_dependency_and_query_dependent() {
    index.trackAction("an-action", "loads");

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> index.trackCustomQuery("a-custom-query", "action", "an-action"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially END and needs to be PRE_QUERIES");
  }

  @Test
  public void rejects_incompatible_query_dependent_and_loads_dependency() {
    index.trackCustomQuery("a-custom-query", "action", "an-action");

    var exception =
        assertThrows(IllegalArgumentException.class, () -> index.trackAction("an-action", "loads"));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Cannot reconcile stage for action \"an-action\": it was initially PRE_QUERIES and needs to be END");
  }
}
