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
package com.google.cloud.teleport.v2.neo4j.model.job;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runtime options object that coalesces well-known (readQuery, inputFilePattern) and arbitrary
 * options.
 */
public class OptionsParams implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(OptionsParams.class);

  private String readQuery = "";
  private String inputFilePattern = "";
  private Integer maxTransactionRetryTimeSeconds = 120;
  private final HashMap<String, String> tokenMap = new HashMap<>();

  public OptionsParams() {}

  @JsonIgnore
  public void overlayTokens(String optionsJsonStr) {
    LOG.info("Pipeline options: {}", optionsJsonStr);
    JSONObject optionsJson = new JSONObject(optionsJsonStr);
    Iterator<String> optionsKeys = optionsJson.keys();
    while (optionsKeys.hasNext()) {
      String optionsKey = optionsKeys.next();
      this.tokenMap.put(optionsKey, String.valueOf(optionsJson.opt(optionsKey)));
      if ("readQuery".equals(optionsKey)) {
        this.readQuery = optionsJson.getString("readQuery");
      } else if ("inputFilePattern".equals(optionsKey)) {
        this.inputFilePattern = optionsJson.getString("inputFilePattern");
      }
      LOG.info("{}: {}", optionsKey, optionsJson.opt(optionsKey));
    }
  }

  public String getReadQuery() {
    return readQuery;
  }

  public void setReadQuery(String readQuery) {
    this.readQuery = readQuery;
  }

  public String getInputFilePattern() {
    return inputFilePattern;
  }

  public void setInputFilePattern(String inputFilePattern) {
    this.inputFilePattern = inputFilePattern;
  }

  public Map<String, String> getTokenMap() {
    return tokenMap;
  }

  public Integer getMaxTransactionRetryTimeSeconds() {
    return maxTransactionRetryTimeSeconds;
  }

  public void setMaxTransactionRetryTimeSeconds(Integer maxTransactionRetryTimeSeconds) {
    this.maxTransactionRetryTimeSeconds = maxTransactionRetryTimeSeconds;
  }
}
