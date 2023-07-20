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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A helper class to validate DataFlow run-time inputs. */
public class InputValidator {

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

    Set<String> sourceNames = new HashSet<>();
    // Source validation
    for (Source source : jobSpec.getSourceList()) {
      String sourceName = source.getName();
      if (StringUtils.isBlank(sourceName)) {
        validationMessages.add("Source is not named");
      }
      if (sourceNames.contains(sourceName)) {
        validationMessages.add("Duplicate source name: " + sourceName);
      } else {
        sourceNames.add(sourceName);
      }
      // Check that SQL does not have order by...
      if (StringUtils.isNotBlank(source.getQuery())) {
        LOG.info("Checking source for ORDER BY");
        Matcher m = ORDER_BY_PATTERN.matcher(source.getQuery());
        if (m.find()) {
          validationMessages.add("SQL contains ORDER BY which is not supported");
        }
      }
    }

    Set<String> targetNames = new HashSet<>();
    // Target validation
    for (Target target : jobSpec.getTargets()) {
      // Check that all targets have names
      if (StringUtils.isBlank(target.getName())) {
        validationMessages.add("Targets must include a 'name' attribute.");
      }
      if (targetNames.contains(target.getName())) {
        validationMessages.add("Duplicate target name: " + target.getName());
      } else {
        targetNames.add(target.getName());
      }
      if (StringUtils.isBlank(target.getSource())) {
        validationMessages.add(
            "Targets must include a 'source' attribute that maps to a 'source.name'.");
      }
      // Check that source exists if defined (otherwise it will be default source)
      if (StringUtils.isNotEmpty(target.getSource())) {
        if (jobSpec.getSourceByName(target.getSource()) == null) {
          validationMessages.add("Target source not defined: " + target.getSource());
        }
      }

      // Check that SQL does not have order by...
      if (target.getTransform() != null && StringUtils.isNotBlank(target.getTransform().getSql())) {
        if (target.getTransform().getSql().toUpperCase().matches("")) {
          Matcher m = ORDER_BY_PATTERN.matcher(target.getTransform().getSql());
          if (m.find()) {
            validationMessages.add(
                "Target " + target.getName() + " SQL contains ORDER BY which is not supported");
          }
        }
      }
      if (target.getType() == TargetType.edge) {
        for (Mapping mapping : target.getMappings()) {
          if (mapping.getFragmentType() == FragmentType.node) {
            validationMessages.add(
                "Invalid fragment type "
                    + mapping.getFragmentType()
                    + " for node mapping: "
                    + mapping.getName());
          }
          if (mapping.getFragmentType() == FragmentType.target
              || mapping.getFragmentType() == FragmentType.source) {
            if (mapping.getRole() != RoleType.key && mapping.getRole() != RoleType.label) {
              validationMessages.add(
                  "Invalid role "
                      + mapping.getRole()
                      + " on relationship: "
                      + mapping.getFragmentType());
            }
          }
        }

        // relationship validation checks..
        if (StringUtils.isBlank(
            ModelUtils.getFirstFieldOrConstant(
                target, FragmentType.source, Arrays.asList(RoleType.key)))) {
          validationMessages.add(
              "Could not find target key field for relationship: " + target.getName());
        }
        if (StringUtils.isBlank(
            ModelUtils.getFirstFieldOrConstant(
                target, FragmentType.target, Arrays.asList(RoleType.key)))) {
          validationMessages.add(
              "Could not find target key field for relationship: " + target.getName());
        }
        if (StringUtils.isBlank(
            ModelUtils.getFirstFieldOrConstant(
                target, FragmentType.rel, Arrays.asList(RoleType.type)))) {
          validationMessages.add("Could not find relationship type: " + target.getName());
        }
      } else if (target.getType() == TargetType.node) {
        for (Mapping mapping : target.getMappings()) {
          if (mapping.getFragmentType() != FragmentType.node) {
            validationMessages.add(
                "Invalid fragment type "
                    + mapping.getFragmentType()
                    + " for node mapping: "
                    + mapping.getName());
          }
        }
        if (StringUtils.isBlank(
            ModelUtils.getFirstFieldOrConstant(
                target, FragmentType.node, Arrays.asList(RoleType.label)))) {
          LOG.info("Invalid target: {}", gson.toJson(target));
          validationMessages.add("Missing label in node: " + target.getName());
        }
        if (StringUtils.isBlank(
            ModelUtils.getFirstFieldOrConstant(
                target, FragmentType.node, Arrays.asList(RoleType.key)))) {
          validationMessages.add("Missing key field in node: " + target.getName());
        }
      }
      // check that calculated fields are used
      if (target.getTransform() != null && !target.getTransform().getAggregations().isEmpty()) {
        for (Aggregation aggregation : target.getTransform().getAggregations()) {
          if (!fieldIsMapped(target, aggregation.getField())) {
            validationMessages.add(
                "Aggregation for field " + aggregation.getField() + " is unmapped.");
          }
        }
      }
    }

    Set<String> actionNames = new HashSet<>();
    if (jobSpec.getActions().size() > 0) {
      // check valid options
      for (Action action : jobSpec.getActions()) {
        String actionName = action.name;
        if (StringUtils.isBlank(actionName)) {
          validationMessages.add("Action is not named");
        }
        if (actionNames.contains(actionName)) {
          validationMessages.add("Duplicate action name: " + actionName);
        } else {
          actionNames.add(actionName);
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
    for (Mapping mapping : target.getMappings()) {
      if (fieldName.equals(mapping.getField())) {
        return true;
      }
    }
    return false;
  }
}
