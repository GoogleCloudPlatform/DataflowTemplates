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
package com.google.cloud.teleport.v2.neo4j.actions;

import com.google.cloud.teleport.v2.neo4j.actions.preload.PreloadAction;
import com.google.cloud.teleport.v2.neo4j.actions.preload.PreloadBigQueryAction;
import com.google.cloud.teleport.v2.neo4j.actions.preload.PreloadCypherAction;
import com.google.cloud.teleport.v2.neo4j.actions.preload.PreloadHttpAction;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import org.neo4j.importer.v1.actions.Action;

/** Factory providing indirection to action handler. */
public class ActionPreloadFactory {

  public static PreloadAction of(Action action, ActionContext context) {
    var actionType = action.getType();
    switch (actionType) {
      case CYPHER:
        PreloadCypherAction cypher = new PreloadCypherAction();
        cypher.configure(action, context);
        return cypher;
      case HTTP:
        PreloadHttpAction http = new PreloadHttpAction();
        http.configure(action, context);
        return http;
      case BIGQUERY:
        PreloadBigQueryAction bigQuery = new PreloadBigQueryAction();
        bigQuery.configure(action, context);
        return bigQuery;
    }
    throw new RuntimeException("Unsupported preload action type: " + actionType);
  }
}
