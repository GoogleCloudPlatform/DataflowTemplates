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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Job specification request object. */
@Getter
public class JobSpec implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(JobSpec.class);

  // initialize defaults;
  private final Map<String, Source> sources = new HashMap<>();
  private final List<Target> targets = new ArrayList<>();
  @Setter private Config config = new Config();
  private final Map<String, String> options = new HashMap<>();
  private final List<Action> actions = new ArrayList<>();

  public List<Target> getActiveTargetsBySource(String sourceName) {
    List<Target> targets = new ArrayList<>();
    for (Target target : this.targets) {
      if (target.active && target.source.equals(sourceName)) {
        targets.add(target);
      }
    }
    return targets;
  }

  public List<Target> getActiveNodeTargetsBySource(String sourceName) {
    List<Target> targets = new ArrayList<>();
    for (Target target : this.targets) {
      if (target.active && target.type == TargetType.node && target.source.equals(sourceName)) {
        targets.add(target);
      }
    }
    return targets;
  }

  public List<Target> getActiveRelationshipTargetsBySource(String sourceName) {
    List<Target> targets = new ArrayList<>();
    for (Target target : this.targets) {
      if (target.active && target.type == TargetType.edge && target.source.equals(sourceName)) {
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
    Iterator<String> sourceKeySet = sources.keySet().iterator();
    while (sourceKeySet.hasNext()) {
      sourceList.add(sources.get(sourceKeySet.next()));
    }
    return sourceList;
  }

  public List<String> getAllFieldNames() {
    ArrayList<String> fieldNameList = new ArrayList<>();
    for (Target target : targets) {
      fieldNameList.addAll(target.fieldNames);
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
}
