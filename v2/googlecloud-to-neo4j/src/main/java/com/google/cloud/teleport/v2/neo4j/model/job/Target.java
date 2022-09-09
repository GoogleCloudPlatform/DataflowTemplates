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
import com.google.cloud.teleport.v2.neo4j.model.enums.SaveMode;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Target (node/edge) metadata. */
@Getter
@Setter
public class Target implements Serializable, Comparable {

  private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
  private static final Logger LOG = LoggerFactory.getLogger(Target.class);
  private String source = "";
  private String name = "";
  private boolean active = true;
  private TargetType type;
  private boolean autoMap = false;
  private Transform transform = new Transform();
  private List<Mapping> mappings = new ArrayList<>();
  private SaveMode saveMode = SaveMode.append;
  private Map<String, Mapping> mappingByFieldMap = new HashMap<>();
  private List<String> fieldNames = new ArrayList<>();
  private int sequence = 0;
  private ActionExecuteAfter executeAfter;
  private String executeAfterName = "";

  public Target() {}

  public Mapping getMappingByFieldName(String fieldName) {
    return this.mappingByFieldMap.get(fieldName);
  }

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
}
