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
import com.google.cloud.teleport.v2.neo4j.actions.function.HttpActionFn;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

/** Factory providing indirection to action handler. */
public class ActionDoFnFactory {

  public static DoFn<Integer, Row> of(ActionContext context) {
    var action = context.getAction();
    var actionType = action.getType();
    switch (actionType) {
      case BIGQUERY:
        return new BigQueryActionFn(context);
      case CYPHER:
        return new CypherActionFn(context);
      case HTTP:
        return new HttpActionFn(context);
    }
    throw new RuntimeException("Unsupported action type: " + action.getClass());
  }
}
