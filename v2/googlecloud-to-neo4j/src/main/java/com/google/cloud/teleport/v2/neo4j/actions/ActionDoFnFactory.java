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

import com.google.cloud.teleport.v2.neo4j.actions.function.BigQueryActionFn;
import com.google.cloud.teleport.v2.neo4j.actions.function.CypherActionFn;
import com.google.cloud.teleport.v2.neo4j.actions.function.HttpGetActionFn;
import com.google.cloud.teleport.v2.neo4j.actions.function.HttpPostActionFn;
import com.google.cloud.teleport.v2.neo4j.model.enums.ActionType;
import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;

/** Factory providing indirection to action handler. */
public class ActionDoFnFactory {

  public static org.apache.beam.sdk.transforms.DoFn<Integer, org.apache.beam.sdk.values.Row> of(
      ActionContext context) {
    Action action = context.action;
    ActionType actionType = action.type;
    if (actionType == ActionType.bigquery) {
      return new BigQueryActionFn(context);
    } else if (actionType == ActionType.cypher) {
      return new CypherActionFn(context);
    } else if (actionType == ActionType.http_post) {
      return new HttpPostActionFn(context);
    } else if (actionType == ActionType.http_get) {
      return new HttpGetActionFn(context);
    } else {
      throw new RuntimeException("Unhandled action type: " + actionType);
    }
  }
}
