/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.it.cassandra;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.io.IOException;
import java.util.concurrent.RejectedExecutionException;
import org.bson.Document;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.testcontainers.containers.CassandraContainer;

/** Unit tests for {@link DefaultCassandraResourceManager}. */
@RunWith(JUnit4.class)
public class DefaultCassandraResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private CqlSession cassandraClient;
  @Mock private CassandraContainer container;

  private static final String TEST_ID = "test-id";
  private static final String COLLECTION_NAME = "collection-name";
  private static final String STATIC_KEYSPACE_NAME = "keyspace";
  private static final String HOST = "localhost";
  private static final int CASSANDRA_PORT = 9042;
  private static final int MAPPED_PORT = 10001;

  private DefaultCassandraResourceManager testManager;

  @Before
  public void setUp() throws IOException, InterruptedException {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(CASSANDRA_PORT)).thenReturn(MAPPED_PORT);

    testManager =
        new DefaultCassandraResourceManager(
            cassandraClient, container, DefaultCassandraResourceManager.builder(TEST_ID));
  }

  @Test
  public void testGetUriShouldReturnCorrectValue() {
    assertThat(testManager.getHost()).matches(HOST);
  }

  @Test
  public void testGetKeyspaceNameShouldReturnCorrectValue() {
    assertThat(testManager.getKeyspaceName()).matches(TEST_ID.replace('-', '_') + "_\\d{8}_\\d{6}");
  }

  @Test
  public void testInsertDocumentsShouldThrowErrorWhenCassandraThrowsException() {

    doThrow(RejectedExecutionException.class)
        .when(cassandraClient)
        .execute(any(SimpleStatement.class));

    assertThrows(
        CassandraResourceManagerException.class,
        () -> testManager.insertDocument(COLLECTION_NAME, new Document()));
  }

  @Test
  public void testCleanupAllShouldNotDropStaticDatabase() throws IOException {
    DefaultCassandraResourceManager.Builder builder =
        DefaultCassandraResourceManager.builder(TEST_ID).setKeyspaceName(STATIC_KEYSPACE_NAME);
    DefaultCassandraResourceManager tm =
        new DefaultCassandraResourceManager(cassandraClient, container, builder);

    tm.cleanupAll();

    verify(cassandraClient, never()).execute(any(SimpleStatement.class));
    verify(cassandraClient).close();
  }

  @Test
  public void testCleanupShouldDropNonStaticDatabase() {

    testManager.cleanupAll();

    verify(cassandraClient).execute(any(SimpleStatement.class));
    verify(cassandraClient).close();
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenCassandraClientFailsToDropDatabase() {
    doThrow(RuntimeException.class).when(cassandraClient).execute(any(SimpleStatement.class));

    assertThrows(CassandraResourceManagerException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenCassandraClientFailsToClose() {
    doThrow(RuntimeException.class).when(cassandraClient).close();

    assertThrows(CassandraResourceManagerException.class, () -> testManager.cleanupAll());
  }
}
