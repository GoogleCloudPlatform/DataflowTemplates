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

import com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.model.enums.ActionType;
import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.Aggregation;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.options.Neo4jFlexTemplateOptions;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A helper class to validate DataFlow run-time inputs. */
public class InputValidator {

  private static final Set<String> validOptions =
      Sets.newHashSet(
          "relationship",
          "relationship.save.strategy",
          "relationship.source.labels",
          "relationship.source.save.mode",
          "relationship.source.node.keys",
          "relationship.target.labels",
          "relationship.target.node.keys",
          "relationship.target.node.properties",
          "relationship.target.save.mode");

  private static final Pattern ORDER_BY_PATTERN = Pattern.compile(".*ORDER\\sBY.*");
  private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
  private static final Logger LOG = LoggerFactory.getLogger(InputValidator.class);

  public static List<String> validateNeo4jPipelineOptions(
      Neo4jFlexTemplateOptions pipelineOptions) {

    List<String> validationMessages = new ArrayList<>();

    if (StringUtils.isEmpty(pipelineOptions.getNeo4jConnectionUri())) {
      validationMessages.add("Neo4j connection URI not provided.");
    }

    if (StringUtils.isEmpty(pipelineOptions.getJobSpecUri())) {
      validationMessages.add("Job spec URI not provided.");
    }

    return validationMessages;
  }

  public static List<String> validateNeo4jConnection(ConnectionParams connectionParams) {
    List<String> validationMessages = new ArrayList<>();
    if (StringUtils.isEmpty(connectionParams.serverUrl)) {
      validationMessages.add("Missing connection server URL");
    }
    if (StringUtils.isEmpty(connectionParams.username)) {
      validationMessages.add("Missing connection username");
    }
    if (StringUtils.isEmpty(connectionParams.password)) {
      validationMessages.add("Missing connection password");
    }
    return validationMessages;
  }

  public static List<String> validateJobSpec(JobSpec jobSpec) {

    List<String> validationMessages = new ArrayList<>();

    // Source validation
    for (Source source : jobSpec.getSourceList()) {
      String sourceName = source.name;
      if (StringUtils.isBlank(sourceName)) {
        validationMessages.add("Source is not named");
      }
      // Check that SQL does not have order by...
      if (StringUtils.isNotBlank(source.query)) {
        LOG.info("Checking source for ORDER BY");
        Matcher m = ORDER_BY_PATTERN.matcher(source.query);
        if (m.find()) {
          validationMessages.add("SQL contains ORDER BY which is not supported");
        }
      }
    }

    // Target validation
    for (Target target : jobSpec.getTargets()) {
      // Check that all targets have names
      if (StringUtils.isBlank(target.name)) {
        validationMessages.add("Targets must include a 'name' attribute.");
      }
      if (StringUtils.isBlank(target.source)) {
        validationMessages.add(
            "Targets must include a 'source' attribute that maps to a 'source.name'.");
      }
      // Check that source exists if defined (otherwise it will be default source)
      if (StringUtils.isNotEmpty(target.source)) {
        if (jobSpec.getSourceByName(target.source) == null) {
          validationMessages.add("Target source not defined: " + target.source);
        }
      }

      // Check that SQL does not have order by...
      if (target.transform != null && StringUtils.isNotBlank(target.transform.sql)) {
        if (target.transform.sql.toUpperCase().matches("")) {
          Matcher m = ORDER_BY_PATTERN.matcher(target.transform.sql);
          if (m.find()) {
            validationMessages.add(
                "Target " + target.name + " SQL contains ORDER BY which is not supported");
          }
        }
      }
      if (target.type == TargetType.edge) {
        for (Mapping mapping : target.mappings) {
          if (mapping.fragmentType == FragmentType.node) {
            validationMessages.add(
                "Invalid fragment type "
                    + mapping.fragmentType
                    + " for node mapping: "
                    + mapping.name);
          }
          if (mapping.fragmentType == FragmentType.target
              || mapping.fragmentType == FragmentType.source) {
            if (mapping.role != RoleType.key && mapping.role != RoleType.label) {
              validationMessages.add(
                  "Invalid role " + mapping.role + " on relationship: " + mapping.fragmentType);
            }
            if (mapping.labels.isEmpty()) {
              validationMessages.add(mapping.fragmentType + " missing label attribute");
            }
          }
        }

        // relationship validation checks..
        if (StringUtils.isBlank(
            ModelUtils.getFirstFieldOrConstant(
                target, FragmentType.source, Arrays.asList(RoleType.key)))) {
          validationMessages.add(
              "Could not find target key field for relationship: " + target.name);
        }
        if (StringUtils.isBlank(
            ModelUtils.getFirstFieldOrConstant(
                target, FragmentType.target, Arrays.asList(RoleType.key)))) {
          validationMessages.add(
              "Could not find target key field for relationship: " + target.name);
        }
        if (StringUtils.isBlank(
            ModelUtils.getFirstFieldOrConstant(
                target, FragmentType.rel, Arrays.asList(RoleType.type)))) {
          validationMessages.add("Could not find relationship type: " + target.name);
        }
      } else if (target.type == TargetType.node) {
        for (Mapping mapping : target.mappings) {
          if (mapping.fragmentType != FragmentType.node) {
            validationMessages.add(
                "Invalid fragment type "
                    + mapping.fragmentType
                    + " for node mapping: "
                    + mapping.name);
          }
        }
        if (StringUtils.isBlank(
            ModelUtils.getFirstFieldOrConstant(
                target, FragmentType.node, Arrays.asList(RoleType.label)))) {
          LOG.info("Invalid target: {}", gson.toJson(target));
          validationMessages.add("Missing label in node: " + target.name);
        }
        if (StringUtils.isBlank(
            ModelUtils.getFirstFieldOrConstant(
                target, FragmentType.node, Arrays.asList(RoleType.key)))) {
          validationMessages.add("Missing key field in node: " + target.name);
        }
      }
      // check that calculated fields are used
      if (target.transform != null && !target.transform.aggregations.isEmpty()) {
        for (Aggregation aggregation : target.transform.aggregations) {
          if (!fieldIsMapped(target, aggregation.field)) {
            validationMessages.add("Aggregation for field " + aggregation.field + " is unmapped.");
          }
        }
      }
    }

    if (jobSpec.getOptions().size() > 0) {
      // check valid options
      Iterator<String> optionIt = jobSpec.getOptions().keySet().iterator();
      while (optionIt.hasNext()) {
        String option = optionIt.next();
        if (!validOptions.contains(option)) {
          validationMessages.add("Invalid option specified: " + option);
        }
      }
    }

    if (jobSpec.getActions().size() > 0) {
      // check valid options
      for (Action action : jobSpec.getActions()) {
        String actionName = action.name;
        if (StringUtils.isBlank(actionName)) {
          validationMessages.add("Action is not named");
        }
        // Check that SQL does not have order by...
        if (action.type == ActionType.cypher) {
          if (!action.options.containsKey("cypher")) {
            validationMessages.add("Parameter 'cypher' is required for cypher-style actions.");
          }
        }
        if (action.type == ActionType.http_get || action.type == ActionType.http_post) {
          if (!action.options.containsKey("url")) {
            validationMessages.add("Parameter 'url' is required for http-style actions.");
          }
        }
        if (action.type == ActionType.bigquery) {
          if (!action.options.containsKey("sql")) {
            validationMessages.add("Parameter 'sql' is required for query-style actions.");
          }
        }
      }
    }

    return validationMessages;
  }

  public static boolean fieldIsMapped(Target target, String fieldName) {
    if (fieldName == null) {
      return false;
    }
    for (Mapping mapping : target.mappings) {
      // LOG.info("Mapping fieldName "+fieldName+": "+gson.toJson(mapping));
      if (fieldName.equals(mapping.field)) {
        return true;
      }
    }
    return false;
  }
}
