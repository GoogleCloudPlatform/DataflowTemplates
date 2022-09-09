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
package com.google.cloud.teleport.v2.neo4j.actions.preload;

import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Cypher runner action handler. */
public class PreloadCypherAction implements PreloadAction {

  private static final Logger LOG = LoggerFactory.getLogger(PreloadCypherAction.class);

  Action action;
  ActionContext context;

  @Override
  public void configure(Action action, ActionContext context) {
    this.action = action;
    this.context = context;
  }

  @Override
  public List<String> execute() {
    List<String> msgs = new ArrayList<>();

    Neo4jConnection directConnect = new Neo4jConnection(this.context.neo4jConnection);
    String cypher = action.options.get("cypher");
    if (StringUtils.isEmpty(cypher)) {
      throw new RuntimeException("Options 'cypher' not provided for preload cypher action.");
    }
    LOG.info("Executing cypher: {}", cypher);
    try {
      directConnect.executeCypher(cypher);
    } catch (Exception e) {
      LOG.error("Exception running cypher, {}: {}", cypher, e.getMessage());
    }

    return msgs;
  }
}
