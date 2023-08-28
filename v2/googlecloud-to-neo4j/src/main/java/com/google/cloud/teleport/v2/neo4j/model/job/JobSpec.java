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
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Job specification request object. */
public class JobSpec implements Serializable {

  // initialize defaults;
  private final Map<String, Source> sources = new LinkedHashMap<>();
  private final List<Target> targets = new ArrayList<>();
  private Config config = new Config();
  private final List<Action> actions = new ArrayList<>();

  public List<Target> getActiveNodeTargetsBySource(String sourceName) {
    List<Target> targets = new ArrayList<>();
    for (Target target : this.targets) {
      if (target.isActive()
          && target.getType() == TargetType.node
          && target.getSource().equals(sourceName)) {
        targets.add(target);
      }
    }
    return targets;
  }

  public List<Target> getActiveRelationshipTargetsBySource(String sourceName) {
    List<Target> targets = new ArrayList<>();
    for (Target target : this.targets) {
      if (target.isActive()
          && target.getType() == TargetType.edge
          && target.getSource().equals(sourceName)) {
        targets.add(target);
      }
    }
    return targets;
  }

  public Source getSourceByName(String name) {
    return sources.get(name);
  }

  public List<Source> getSourceList() {
    ArrayList<Source> sourceList = new ArrayList<>();
    for (String s : sources.keySet()) {
      sourceList.add(sources.get(s));
    }
    return sourceList;
  }

  public List<String> getAllFieldNames() {
    ArrayList<String> fieldNameList = new ArrayList<>();
    for (Target target : targets) {
      fieldNameList.addAll(target.getFieldNames());
    }
    return fieldNameList;
  }

  public List<Action> getPreloadActions() {
    List<Action> actions = new ArrayList<>();
    for (Action action : this.actions) {
      if (action.executeAfter == ActionExecuteAfter.start) {
        actions.add(action);
      }
    }
    return actions;
  }

  public List<Action> getPostloadActions() {
    List<Action> actions = new ArrayList<>();
    for (Action action : this.actions) {
      if (action.executeAfter != ActionExecuteAfter.start) {
        actions.add(action);
      }
    }
    return actions;
  }

  public Map<String, Source> getSources() {
    return sources;
  }

  public List<Target> getTargets() {
    return targets;
  }

  public Config getConfig() {
    return config;
  }

  public void setConfig(Config config) {
    this.config = config;
  }

  public List<Action> getActions() {
    return actions;
  }
}
