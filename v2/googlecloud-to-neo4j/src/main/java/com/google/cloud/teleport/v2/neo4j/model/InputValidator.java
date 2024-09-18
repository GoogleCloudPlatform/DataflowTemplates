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
package com.google.cloud.teleport.v2.neo4j.model;

import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.cloud.teleport.v2.neo4j.model.Json.ParsingResult;
import com.google.cloud.teleport.v2.neo4j.options.Neo4jFlexTemplateOptions;
import com.networknt.schema.JsonSchema;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/** A helper class to validate DataFlow run-time inputs. */
public class InputValidator {

  public static List<String> validateNeo4jPipelineOptions(
      Neo4jFlexTemplateOptions pipelineOptions) {

    List<String> validationMessages = new ArrayList<>(2);

    String neo4jConnectionUri = pipelineOptions.getNeo4jConnectionUri();
    String neo4jConnectionSecret = pipelineOptions.getNeo4jConnectionSecretId();
    if (StringUtils.isEmpty(neo4jConnectionUri) && StringUtils.isEmpty(neo4jConnectionSecret)) {
      validationMessages.add(
          "Neither Neo4j connection URI nor Neo4j connection secret were provided.");
    }
    if (!StringUtils.isEmpty(neo4jConnectionUri) && !StringUtils.isEmpty(neo4jConnectionSecret)) {
      validationMessages.add(
          "Both Neo4j connection URI and Neo4j connection secret were provided: only one must be set.");
    }
    if (!StringUtils.isEmpty(neo4jConnectionSecret)
        && !(SecretVersionName.isParsableFrom(neo4jConnectionSecret))) {
      validationMessages.add(
          "Neo4j connection secret must be in the form"
              + " projects/{project}/secrets/{secret}/versions/{secret_version}");
    }
    if (StringUtils.isEmpty(pipelineOptions.getJobSpecUri())) {
      validationMessages.add("Job spec URI not provided.");
    }

    return validationMessages;
  }

  public static ParsingResult validateNeo4jConnection(String json) {
    JsonSchema connectionSchema =
        Json.SCHEMA_FACTORY.getSchema(
            InputValidator.class.getResourceAsStream("/schemas/connection.v1.0.json"));
    return Json.parseAndValidate(json, connectionSchema);
  }
}
