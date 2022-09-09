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
package com.google.cloud.teleport.v2.neo4j.model.helpers;

import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.options.Neo4jFlexTemplateOptions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class for parsing json into OptionsParams model object. */
public class OptionsParamsMapper {

  private static final Logger LOG = LoggerFactory.getLogger(OptionsParamsMapper.class);

  public static OptionsParams fromPipelineOptions(Neo4jFlexTemplateOptions pipelineOptions) {
    OptionsParams optionsParams = new OptionsParams();
    try {

      if (StringUtils.isNotEmpty(pipelineOptions.getReadQuery())) {
        optionsParams.setReadQuery(pipelineOptions.getReadQuery());
      }
      if (StringUtils.isNotEmpty(pipelineOptions.getInputFilePattern())) {
        optionsParams.setInputFilePattern(pipelineOptions.getInputFilePattern());
      }
      optionsParams.overlayTokens(pipelineOptions.getOptionsJson());

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return optionsParams;
  }
}
