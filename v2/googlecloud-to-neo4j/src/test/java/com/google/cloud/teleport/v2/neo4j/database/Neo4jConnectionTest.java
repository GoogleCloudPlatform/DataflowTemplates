/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.database;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

@RunWith(JUnit4.class)
public class Neo4jConnectionTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private Driver driver;
  @Mock private Session session;
  @Mock private Result result;

  private Neo4jConnection neo4jConnection;

  @Before
  public void setUp() {
    when(session.run(anyString())).thenReturn(result);
    when(session.run(anyString(), anyMap())).thenReturn(result);
    when(driver.session(any())).thenReturn(session);
    neo4jConnection = new Neo4jConnection("a-database", () -> driver);
  }

  @Test
  public void resetsDatabaseByRecreatingIt() {
    neo4jConnection.resetDatabase();

    verify(session).run("CREATE OR REPLACE DATABASE $db", Map.of("db", "a-database"));
    verify(session, never()).run("MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS");
    verify(session, never()).run("CALL apoc.schema.assert({}, {}, true)");
  }

  @Test
  public void resetsDatabaseWithDeletionQueriesWhenReplacementFails() {
    when(session.run("CREATE OR REPLACE DATABASE $db", Map.of("db", "a-database")))
        .thenThrow(RuntimeException.class);

    neo4jConnection.resetDatabase();

    InOrder inOrder = inOrder(session);
    inOrder.verify(session).run("CREATE OR REPLACE DATABASE $db", Map.of("db", "a-database"));
    inOrder
        .verify(session)
        .run("MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS", Map.of());
    inOrder.verify(session).run("CALL apoc.schema.assert({}, {}, true)", Map.of());
  }
}
