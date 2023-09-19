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

import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InputRefactoring {

  private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
  private static final Logger LOG = LoggerFactory.getLogger(InputRefactoring.class);
  OptionsParams optionsParams;

  private InputRefactoring() {}

  /** Constructor uses template input options. */
  public InputRefactoring(OptionsParams optionsParams) {
    this.optionsParams = optionsParams;
  }

  /**
   * Transforms and simplifies the provided job specification. This takes care of interpolating
   * runtime tokens in relevant areas, setting sensible defaults and simplifying target mappings
   * when applicable
   *
   * @param jobSpec a JobSpec instance, first validated by {@link InputValidator}
   */
  public void refactorJobSpec(JobSpec jobSpec) {
    LOG.info("Options params: {}", gson.toJson(optionsParams));

    // replace URI and SQL with run-time options
    for (Source source : jobSpec.getSourceList()) {
      rewriteSource(source);
    }

    for (Action action : jobSpec.getActions()) {
      rewriteAction(action);
    }

    int targetNum = 0;
    List<Target> activeTargets =
        jobSpec.getTargets().stream().filter(Target::isActive).collect(Collectors.toList());
    List<Target> rewrittenTargets = new ArrayList<>(activeTargets.size());
    for (Target target : activeTargets) {
      targetNum++;
      target.setSequence(targetNum);
      if (StringUtils.isEmpty(target.getName())) {
        target.setName("Target " + targetNum);
      }
      if (target.getType() == TargetType.custom_query) {
        String customQuery = target.getCustomQuery();
        target.setCustomQuery(
            ModelUtils.replaceVariableTokens(customQuery, optionsParams.getTokenMap()));
      }
      rewriteTargetMappings(target, jobSpec.getConfig().getIndexAllProperties());
      rewrittenTargets.add(target);
    }
    jobSpec.setTargets(rewrittenTargets);
  }

  public void optimizeJobSpec(JobSpec jobSpec) {

    // NODES first then relationships
    // This does not actually change execution order, just numbering
    Collections.sort(jobSpec.getTargets());
  }

  private void rewriteSource(Source source) {

    // rewrite file URI
    String dataFileUri = source.getUri();
    if (StringUtils.isNotEmpty(optionsParams.getInputFilePattern())) {
      LOG.info("Overriding source uri with run-time option");
      dataFileUri = optionsParams.getInputFilePattern();
    }
    source.setUri(ModelUtils.replaceVariableTokens(dataFileUri, optionsParams.getTokenMap()));

    // rewrite SQL
    String sql = source.getQuery();
    if (StringUtils.isNotEmpty(optionsParams.getReadQuery())) {
      LOG.info("Overriding sql with run-time option");
      sql = optionsParams.getReadQuery();
    }
    source.setQuery(ModelUtils.replaceVariableTokens(sql, optionsParams.getTokenMap()));
  }

  private void rewriteAction(Action action) {
    for (Entry<String, String> entry : action.options.entrySet()) {
      String value = entry.getValue();
      action.options.put(
          entry.getKey(), ModelUtils.replaceVariableTokens(value, optionsParams.getTokenMap()));
    }
  }

  private void rewriteTargetMappings(Target target, boolean indexAllProperties) {
    List<Mapping> mappings = target.getMappings();
    Predicate<Mapping> mappingIsRewriteCandidate = mappingIsRewriteCandidate(target);
    List<Mapping> unprocessedMappings =
        mappings.stream()
            .filter(mappingIsRewriteCandidate.negate())
            .collect(Collectors.toCollection(ArrayList::new));
    List<Mapping> rewrittenMappings =
        mappings.stream()
            .filter(mappingIsRewriteCandidate)
            .collect(Collectors.groupingBy(Mapping::getName))
            .values()
            .stream()
            .map(
                propertyMappings ->
                    propertyMappings.stream()
                        .reduce(Mapping::mergeOverlapping)
                        .map(m -> overwriteIndexed(m, indexAllProperties))
                        .get())
            .collect(Collectors.toList());

    List<Mapping> result = new ArrayList<>(unprocessedMappings.size() + rewrittenMappings.size());
    result.addAll(unprocessedMappings);
    result.addAll(rewrittenMappings);
    target.setMappings(result);
  }

  private static Predicate<Mapping> mappingIsRewriteCandidate(Target target) {
    return mapping -> {
      RoleType role = mapping.getRole();
      if (role != RoleType.property && role != RoleType.key) {
        // not much to optimize for labels and types
        return false;
      }
      if (target.getType() == TargetType.node) {
        // need to inspect node property mappings
        return true;
      }
      // start/end node keys are not looked at atm
      // only key/properties of the relationship itself are inspected
      return mapping.getFragmentType() == FragmentType.rel;
    };
  }

  private static Mapping overwriteIndexed(Mapping mapping, boolean indexAllProperties) {
    if (mapping.getRole() == RoleType.key) {
      // key constraints are backed by an index, no index statement needed
      return mapping;
    }
    if (mapping.isUnique()) {
      // unique constraints are backed by an index, no index statement needed
      return mapping;
    }
    if (mapping.isIndexed()) {
      // mapping is already indexed, moving on
      return mapping;
    }
    mapping.setIndexed(indexAllProperties);
    return mapping;
  }
}
