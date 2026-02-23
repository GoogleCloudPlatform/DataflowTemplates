/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.actions;

import java.util.Map;
import org.neo4j.importer.v1.actions.Action;
import org.neo4j.importer.v1.actions.ActionStage;

public record HttpAction(
    boolean active,
    String name,
    ActionStage stage,
    String url,
    HttpMethod method,
    Map<String, String> headers)
    implements Action {

  @Override
  public String getType() {
    return "http";
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean isActive() {
    return active;
  }

  @Override
  public ActionStage getStage() {
    return stage;
  }
}
