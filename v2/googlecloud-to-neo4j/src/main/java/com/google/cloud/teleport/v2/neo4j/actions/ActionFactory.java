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
import com.google.cloud.teleport.v2.neo4j.actions.preload.PreloadHttpGetAction;
import com.google.cloud.teleport.v2.neo4j.actions.preload.PreloadHttpPostAction;
import com.google.cloud.teleport.v2.neo4j.model.enums.ActionType;
import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;

/** Factory providing indirection to action handler. */
public class ActionFactory {

  public static PreloadAction of(Action action, ActionContext context) {
    ActionType actionType = action.type;
    if (actionType == ActionType.bigquery) {
      PreloadBigQueryAction impl = new PreloadBigQueryAction();
      impl.configure(action, context);
      return impl;
    } else if (actionType == ActionType.cypher) {
      PreloadCypherAction impl = new PreloadCypherAction();
      impl.configure(action, context);
      return impl;
    } else if (actionType == ActionType.http_post) {
      PreloadHttpPostAction impl = new PreloadHttpPostAction();
      impl.configure(action, context);
      return impl;
    } else if (actionType == ActionType.http_get) {
      PreloadHttpGetAction impl = new PreloadHttpGetAction();
      impl.configure(action, context);
      return impl;
    } else {
      throw new RuntimeException("Unhandled action type: " + actionType);
    }
  }
}
