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
package com.google.cloud.teleport.v2.neo4j.model.enums;

/**
 * Allows creation of run-time dependencies.
 *
 * <p>These are execution stages: start, sources, nodes, edges, loads, preloads
 *
 * <p>These refer to specific items when qualified with a name:
 *
 * <p>source, node, edge,
 *
 * <p>Currently: async==start==preloads
 */
public enum ActionExecuteAfter {
  start, // after initialization
  sources, // after sources
  nodes,
  edges,
  source,
  node,
  edge,
  custom_queries,
  action, // after action
  async,
  loads,
  preloads
}
