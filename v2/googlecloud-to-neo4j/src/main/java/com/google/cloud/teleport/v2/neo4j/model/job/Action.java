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
import com.google.cloud.teleport.v2.neo4j.model.enums.ActionType;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/** Pre and post-load action model object. */
public class Action implements Serializable {

  // TODO: add run-log as input to actions.
  // Extent providers to emit structured run-log.

  public String name = "";
  public String description = "";
  public ActionType type = ActionType.cypher;
  public Map<String, String> options = new HashMap<>();
  public Map<String, String> headers = new HashMap<>();

  public ActionExecuteAfter executeAfter = ActionExecuteAfter.edges;
  public String executeAfterName = "";

  ////////////////////////
  // supported options
  //
  // actiontype: query
  // note: query cannot return values
  // "source" - name of source
  // "sql" - sql to execute on source
  //
  // actiontype: http_get, http_post
  // "url" - name of source
  // <other keys are passed in payload or query string as key-value>
  // for GET params are converted to query strings
  // for POST params are converted to entity key-value pair JSON
  // headers map passed as key-values
  //
  // actiontype: cypher
  // "cypher" - name of source
  // <other keys are passed in payload or query string as key-value>
  // headers map passed as headers

}
