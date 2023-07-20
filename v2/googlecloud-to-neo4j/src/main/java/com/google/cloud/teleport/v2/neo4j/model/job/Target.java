/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.model.job;

import com.google.cloud.teleport.v2.neo4j.model.enums.ActionExecuteAfter;
import com.google.cloud.teleport.v2.neo4j.model.enums.EdgeNodesMatchMode;
import com.google.cloud.teleport.v2.neo4j.model.enums.SaveMode;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Target (node/edge) metadata. */
public class Target implements Serializable, Comparable {

  private String source = "";
  private String name = "";
  private boolean active = true;
  private TargetType type;
  private Transform transform = new Transform();
  private List<Mapping> mappings = new ArrayList<>();
  private SaveMode saveMode = SaveMode.append;

  private EdgeNodesMatchMode edgeNodesMatchMode;
  private Map<String, Mapping> mappingByFieldMap = new HashMap<>();
  private List<String> fieldNames = new ArrayList<>();
  private int sequence = 0;
  private ActionExecuteAfter executeAfter;
  private String executeAfterName = "";

  public Target() {}

  @Override
  public int compareTo(Object o) {
    if (this.type == ((Target) o).type) {
      return 0;
    } else if (this.type == TargetType.edge && ((Target) o).type == TargetType.node) {
      return 1;
    } else {
      return -1;
    }
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public TargetType getType() {
    return type;
  }

  public void setType(TargetType type) {
    this.type = type;
  }

  public Transform getTransform() {
    return transform;
  }

  public List<Mapping> getMappings() {
    return mappings;
  }

  public void setMappings(List<Mapping> mappings) {
    this.mappings = mappings;
  }

  public SaveMode getSaveMode() {
    return saveMode;
  }

  public void setSaveMode(SaveMode saveMode) {
    this.saveMode = saveMode;
  }

  public EdgeNodesMatchMode getEdgeNodesMatchMode() {
    return edgeNodesMatchMode;
  }

  public void setEdgeNodesMatchMode(EdgeNodesMatchMode edgeNodesMatchMode) {
    this.edgeNodesMatchMode = edgeNodesMatchMode;
  }

  public Map<String, Mapping> getMappingByFieldMap() {
    return mappingByFieldMap;
  }

  public List<String> getFieldNames() {
    return fieldNames;
  }

  public void setFieldNames(List<String> fieldNames) {
    this.fieldNames = fieldNames;
  }

  public int getSequence() {
    return sequence;
  }

  public void setSequence(int sequence) {
    this.sequence = sequence;
  }

  public ActionExecuteAfter getExecuteAfter() {
    return executeAfter;
  }

  public void setExecuteAfter(ActionExecuteAfter executeAfter) {
    this.executeAfter = executeAfter;
  }

  public String getExecuteAfterName() {
    return executeAfterName;
  }

  public void setExecuteAfterName(String executeAfterName) {
    this.executeAfterName = executeAfterName;
  }
}
