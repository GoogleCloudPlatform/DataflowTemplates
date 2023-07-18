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
package com.google.cloud.teleport.it.neo4j;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.testcontainers.TestContainersIntegrationTest;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link Neo4jResourceManager}. */
@Category(TestContainersIntegrationTest.class)
@RunWith(JUnit4.class)
public class Neo4jResourceManagerIT {

  private Neo4jResourceManager neo4jResourceManager;

  @Before
  public void setUp() {
    neo4jResourceManager =
        Neo4jResourceManager.builder("placeholder")
            .setDatabaseName("neo4j")
            .setAdminPassword("password")
            .build();
  }

  @Test
  public void testResourceManagerE2E() {
    neo4jResourceManager.run("CREATE (:Hello {whom: $whom})", Map.of("whom", "world"));

    List<Map<String, Object>> results =
        neo4jResourceManager.run("MATCH (h:Hello) RETURN h.whom AS whom");

    assertThat(results).hasSize(1);
    assertThat(results).containsExactlyElementsIn(List.of(Map.of("whom", "world")));
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(neo4jResourceManager);
  }
}
