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
package com.google.cloud.teleport.it.gcp.datastore;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.datastore.Entity;
import com.google.cloud.teleport.it.common.TestProperties;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/** Integration tests for {@link DatastoreResourceManager}. */
public class DatastoreResourceManagerIT {

  @Test
  public void testInsert() throws IOException {
    DatastoreResourceManager resourceManager =
        DatastoreResourceManager.builder(
                TestProperties.project(), DatastoreUtils.createTestId("testInsert"))
            .credentials(TestProperties.credentials())
            .build();
    List<Entity> entities =
        resourceManager.insert(
            "person",
            Map.of(
                1L,
                Entity.newBuilder().set("name", "John Doe").build(),
                2L,
                Entity.newBuilder().set("name", "Joan of Arc").build()));
    assertThat(entities).hasSize(2);

    resourceManager.cleanupAll();
  }

  @Test
  public void testInsertQuery() throws IOException {
    DatastoreResourceManager resourceManager =
        DatastoreResourceManager.builder(
                TestProperties.project(), DatastoreUtils.createTestId("testInsertQuery"))
            .credentials(TestProperties.buildCredentialsFromEnv())
            .build();

    List<Entity> entities =
        resourceManager.insert(
            "person", Map.of(1L, Entity.newBuilder().set("name", "John Doe").build()));

    assertThat(entities).hasSize(1);
    List<Entity> queryResults = resourceManager.query("SELECT * from person");
    assertThat(queryResults).isNotEmpty();
    Entity person = queryResults.get(0);
    assertThat(person).isNotNull();
    assertThat(person.getKey().getId()).isEqualTo(1L);
    assertThat(person.getString("name")).isEqualTo("John Doe");

    resourceManager.cleanupAll();
  }

  @Test
  public void testInsertCleanUp() throws IOException {
    DatastoreResourceManager resourceManager =
        DatastoreResourceManager.builder(
                TestProperties.project(), DatastoreUtils.createTestId("testInsertCleanUp"))
            .credentials(TestProperties.buildCredentialsFromEnv())
            .build();
    resourceManager.insert(
        "person", Map.of(1L, Entity.newBuilder().set("name", "John Doe").build()));

    resourceManager.cleanupAll();

    List<Entity> queryResults = resourceManager.query("SELECT * from person");
    assertThat(queryResults).isEmpty();
  }
}
