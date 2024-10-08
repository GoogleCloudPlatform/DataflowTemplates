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

/** Runtime options object that coalesces arbitrary options. */
public class OptionsParams implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(OptionsParams.class);

  private final Map<String, String> tokenMap = new HashMap<>();

  @JsonIgnore
  public void overlayTokens(String optionsJsonStr) {
    LOG.debug("Parsing pipeline options: {}", optionsJsonStr);
    JSONObject optionsJson = new JSONObject(optionsJsonStr);
    Iterator<String> optionsKeys = optionsJson.keys();
    while (optionsKeys.hasNext()) {
      String optionsKey = optionsKeys.next();
      this.tokenMap.put(optionsKey, String.valueOf(optionsJson.opt(optionsKey)));
      LOG.debug("{}: {}", optionsKey, optionsJson.opt(optionsKey));
    }
  }

  public Map<String, String> getTokenMap() {
    return tokenMap;
  }
}
