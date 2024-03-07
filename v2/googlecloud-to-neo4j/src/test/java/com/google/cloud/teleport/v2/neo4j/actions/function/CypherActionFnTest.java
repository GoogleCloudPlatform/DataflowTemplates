/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.actions.function;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.junit.Test;
import org.neo4j.driver.TransactionConfig;

public class CypherActionFnTest {

  @Test
  public void sendsTransactionMetadata() {
    Neo4jConnection connection = mock(Neo4jConnection.class);
    ActionContext context = new ActionContext();
    Action action = new Action();
    action.options.put("cypher", "RETURN 42");
    context.action = action;
    CypherActionFn actionFn = new CypherActionFn(context, () -> connection);
    actionFn.setup();

    actionFn.processElement(mock(ProcessContext.class));

    Map<String, String> expectedTxMetadata = Map.of("sink", "neo4j", "step", "cypher-action");
    TransactionConfig expectedTransactionConfig =
        TransactionConfig.builder()
            .withMetadata(Map.of("app", "dataflow", "metadata", expectedTxMetadata))
            .build();
    verify(connection).executeCypher("RETURN 42", expectedTransactionConfig);
  }
}
