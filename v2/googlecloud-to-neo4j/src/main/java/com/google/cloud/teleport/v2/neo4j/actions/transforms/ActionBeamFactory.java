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
package com.google.cloud.teleport.v2.neo4j.actions.transforms;

/** Factory providing indirection to action handler. */
public class ActionBeamFactory {

  public static org.apache.beam.sdk.transforms.PTransform<org.apache.beam.sdk.values.PCollection<org.apache.beam.sdk.values.Row>, org.apache.beam.sdk.values.PCollection<org.apache.beam.sdk.values.Row>> of(
      com.google.cloud.teleport.v2.neo4j.model.job.ActionContext context) {
    com.google.cloud.teleport.v2.neo4j.model.job.Action action=context.action;
    com.google.cloud.teleport.v2.neo4j.model.enums.ActionType actionType = action.type;
    if (actionType == com.google.cloud.teleport.v2.neo4j.model.enums.ActionType.bigquery) {
      return new BigQueryOneActionTransform( context);
    } else if (actionType == com.google.cloud.teleport.v2.neo4j.model.enums.ActionType.cypher) {
      return new CypherOneActionTransform( context);
    } else if (actionType == com.google.cloud.teleport.v2.neo4j.model.enums.ActionType.http_post) {
      return new HttpPostOneActionTransform( context);
    } else if (actionType == com.google.cloud.teleport.v2.neo4j.model.enums.ActionType.http_get) {
      return new HttpGetOneActionTransform( context);
    } else {
      throw new RuntimeException("Unhandled action type: " + actionType);
    }
  }
}
