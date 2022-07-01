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

import com.google.cloud.teleport.v2.neo4j.actions.transforms.BigQueryActionTransform;
import com.google.cloud.teleport.v2.neo4j.actions.transforms.CypherActionTransform;
import com.google.cloud.teleport.v2.neo4j.actions.transforms.HttpGetActionTransform;
import com.google.cloud.teleport.v2.neo4j.actions.transforms.HttpPostActionTransform;
import com.google.cloud.teleport.v2.neo4j.model.enums.ActionType;
import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/** Factory providing indirection to action handler. */
public class ActionBeamFactory {
  public static PTransform<PCollection<Row>, PCollection<Row>> of(
      Action action, ActionContext context) {
    ActionType actionType = action.type;
    if (actionType == ActionType.bigquery) {
      return new BigQueryActionTransform(action, context);
    } else if (actionType == ActionType.cypher) {
      return new CypherActionTransform(action, context);
    } else if (actionType == ActionType.http_post) {
      return new HttpPostActionTransform(action, context);
    } else if (actionType == ActionType.http_get) {
      return new HttpGetActionTransform(action, context);
    } else {
      throw new RuntimeException("Unhandled action type: " + actionType);
    }
  }
}
