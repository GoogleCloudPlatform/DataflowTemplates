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
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
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
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.InternalRecord;

@RunWith(JUnit4.class)
public class Neo4jConnectionTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private Driver driver;
  @Mock private Session session;
  @Mock private Result result;

  private Neo4jConnection neo4jConnection;

  @Before
  public void setUp() {
    when(session.run(anyString(), anyMap(), any())).thenReturn(result);
    when(driver.session(any())).thenReturn(session);
    neo4jConnection = new Neo4jConnection("a-database", () -> driver);
  }

  @Test
  public void resetsDatabaseByRecreatingItOnEnterprise() {
    setVersionEdition("5.1.0", "enterprise");

    neo4jConnection.resetDatabase();

    verify(session)
        .run(
            eq("CREATE OR REPLACE DATABASE $db WAIT 60 SECONDS"),
            eq(Map.of("db", "a-database")),
            any());
    verify(session, never())
        .run(eq("MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"), anyMap(), any());
    verify(session, never()).run(contains("CONSTRAINT"), anyMap(), any());
    verify(session, never()).run(contains("INDEX"), anyMap(), any());
  }

  @Test
  public void resetsDatabaseByRecreatingItOnAura() {
    setVersionEdition("5.1-aura", "enterprise");
    setConstraints("a", "b");
    setIndexes("c", "d");

    neo4jConnection.resetDatabase();

    InOrder inOrder = inOrder(session);
    inOrder
        .verify(session)
        .run(eq("MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"), eq(Map.of()), any());
    inOrder.verify(session).run(eq("SHOW CONSTRAINTS YIELD name"), eq(Map.of()), any());
    inOrder.verify(session).run(eq("DROP CONSTRAINT `a`"), eq(Map.of()), any());
    inOrder.verify(session).run(eq("DROP CONSTRAINT `b`"), eq(Map.of()), any());
    inOrder
        .verify(session)
        .run(
            eq("SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name"),
            eq(Map.of()),
            any());
    inOrder.verify(session).run(eq("DROP INDEX `c`"), eq(Map.of()), any());
    inOrder.verify(session).run(eq("DROP INDEX `d`"), eq(Map.of()), any());
  }

  @Test
  public void resetsDatabaseByRecreatingItOnCommunity() {
    setVersionEdition("5.1.0", "community");
    setIndexes("c", "d");

    neo4jConnection.resetDatabase();

    InOrder inOrder = inOrder(session);
    inOrder
        .verify(session)
        .run(eq("MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"), eq(Map.of()), any());
    inOrder
        .verify(session)
        .run(
            eq("SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name"),
            eq(Map.of()),
            any());
    inOrder.verify(session).run(eq("DROP INDEX `c`"), eq(Map.of()), any());
    inOrder.verify(session).run(eq("DROP INDEX `d`"), eq(Map.of()), any());

    verify(session, never()).run(contains("CONSTRAINT"), anyMap(), any());
  }

  @Test
  public void resetsDatabaseWithDeletionQueriesWhenReplacementFailsOnEnterprise() {
    setVersionEdition("5.1.0", "enterprise");
    setConstraints("a", "b");
    setIndexes("c", "d");

    when(session.run(
            eq("CREATE OR REPLACE DATABASE $db WAIT 60 SECONDS"),
            eq(Map.of("db", "a-database")),
            any()))
        .thenThrow(RuntimeException.class);

    neo4jConnection.resetDatabase();

    InOrder inOrder = inOrder(session);
    inOrder
        .verify(session)
        .run(eq("MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"), eq(Map.of()), any());
    inOrder.verify(session).run(eq("SHOW CONSTRAINTS YIELD name"), eq(Map.of()), any());
    inOrder.verify(session).run(eq("DROP CONSTRAINT `a`"), eq(Map.of()), any());
    inOrder.verify(session).run(eq("DROP CONSTRAINT `b`"), eq(Map.of()), any());
    inOrder
        .verify(session)
        .run(
            eq("SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name"),
            eq(Map.of()),
            any());
    inOrder.verify(session).run(eq("DROP INDEX `c`"), eq(Map.of()), any());
    inOrder.verify(session).run(eq("DROP INDEX `d`"), eq(Map.of()), any());
  }

  private void setVersionEdition(String version, String edition) {
    var result = mock(Result.class);
    when(result.single())
        .thenReturn(
            new InternalRecord(
                List.of("version", "edition"),
                new Value[] {Values.value(version), Values.value(edition)}));

    when(session.run(contains("dbms.components"), anyMap())).thenReturn(result);
  }

  private void setConstraints(String... names) {
    var result = mock(Result.class);
    when(result.list(any())).thenReturn(Arrays.asList(names));

    when(session.run(eq("SHOW CONSTRAINTS YIELD name"), anyMap(), any())).thenReturn(result);
  }

  private void setIndexes(String... names) {
    var result = mock(Result.class);
    when(result.list(any())).thenReturn(Arrays.asList(names));

    when(session.run(
            eq("SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name"),
            anyMap(),
            any()))
        .thenReturn(result);
  }
}
