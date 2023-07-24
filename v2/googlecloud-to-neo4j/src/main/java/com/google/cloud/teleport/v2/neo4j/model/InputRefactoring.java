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

import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.Collections;
import java.util.Map.Entry;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Synthesizes and optimizes missing elements to model from inputs. */
public class InputRefactoring {

  private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
  private static final Logger LOG = LoggerFactory.getLogger(InputRefactoring.class);
  OptionsParams optionsParams;

  private InputRefactoring() {}

  /** Constructor uses template input options. */
  public InputRefactoring(OptionsParams optionsParams) {
    this.optionsParams = optionsParams;
  }

  public void refactorJobSpec(JobSpec jobSpec) {

    // targets defined but no field names defined.
    if (jobSpec.getAllFieldNames().isEmpty()) {
      LOG.error("Targets not found, synthesizing from source is not currently supported.");
      throw new RuntimeException("Not currently auto-generating targets.");
    }

    LOG.info("Options params: {}", gson.toJson(optionsParams));

    // replace URI and SQL with run-time options
    for (Source source : jobSpec.getSourceList()) {
      rewriteSource(source);
    }

    for (Action action : jobSpec.getActions()) {
      rewriteAction(action);
    }

    // number and name targets
    int targetNum = 0;
    for (Target target : jobSpec.getTargets()) {
      targetNum++;
      target.setSequence(targetNum);
      if (StringUtils.isEmpty(target.getName())) {
        target.setName("Target " + targetNum);
      }
    }
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
}
