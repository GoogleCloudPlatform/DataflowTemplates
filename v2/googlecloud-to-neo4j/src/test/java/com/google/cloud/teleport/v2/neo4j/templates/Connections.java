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

import org.apache.beam.it.neo4j.Neo4jResourceManager;

class Connections {

  public static String jsonBasicPayload(Neo4jResourceManager neo4jClient) {
    return String.format(
        "{\n"
            + "  \"server_url\": \"%s\",\n"
            + "  \"database\": \"%s\",\n"
            + "  \"auth_type\": \"basic\",\n"
            + "  \"username\": \"neo4j\",\n"
            + "  \"pwd\": \"%s\"\n"
            + "}",
        neo4jClient.getUri(), neo4jClient.getDatabaseName(), neo4jClient.getAdminPassword());
  }
}
