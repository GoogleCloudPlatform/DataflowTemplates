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
package com.google.cloud.teleport.v2.neo4j.actions.doFn;

public class CypherActionFn extends org.apache.beam.sdk.transforms.DoFn<Integer, Void> {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CypherActionFn.class);

    private final com.google.cloud.teleport.v2.neo4j.model.job.ActionContext context;
    private final com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams connectionParams;
    private final String cypher;

    private com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection directConnect;

    public CypherActionFn(com.google.cloud.teleport.v2.neo4j.model.job.ActionContext context) {
        this.context = context;
        this.connectionParams = context.neo4jConnectionParams;
        this.cypher = this.context.action.options.get("cypher");
        if (org.apache.commons.lang3.StringUtils.isEmpty(cypher)) {
            throw new RuntimeException("Options 'cypher' not provided for cypher action transform.");
        }
    }

    @Setup
    public void setup() {
        directConnect = new com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection(connectionParams);
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws InterruptedException {
        LOG.info("Executing cypher action transform: {}", cypher);
        try {
            directConnect.executeCypher(cypher);
        } catch (Exception e) {
            LOG.error("Exception running cypher transform, {}: {}", cypher, e.getMessage());
        }
    }

    @Teardown
    public void tearDown() throws Exception {
        if (directConnect != null && directConnect.getSession().isOpen()) {
            directConnect.getSession().close();
        }
    }
}
