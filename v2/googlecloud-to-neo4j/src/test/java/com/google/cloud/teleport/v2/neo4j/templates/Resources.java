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
package com.google.cloud.teleport.v2.neo4j.templates;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.neo4j.Neo4jResourceManager;

class Resources {

  public static String contentOf(String resourcePath) throws IOException {
    try (BufferedReader bufferedReader =
        new BufferedReader(
            new InputStreamReader(Resources.class.getResourceAsStream(resourcePath)))) {
      return bufferedReader.lines().collect(Collectors.joining("\n"));
    }
  }

  // TODO: move this to neo4j resource manager in Beam
  public static void cleanUpDataBase(Neo4jResourceManager neo4jClient) {
    neo4jClient.run("MATCH (n) DETACH DELETE n;");
    List<Map<String, Object>> results = neo4jClient.run("SHOW CONSTRAINTS YIELD name");
    for (Map<String, Object> result : results) {
      String name = (String) result.get("name");
      neo4jClient.run("DROP CONSTRAINT " + name);
    }
    results = neo4jClient.run("SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' ");
    for (Map<String, Object> result : results) {
      String name = (String) result.get("name");
      neo4jClient.run("DROP INDEX " + name);
    }
  }
}
