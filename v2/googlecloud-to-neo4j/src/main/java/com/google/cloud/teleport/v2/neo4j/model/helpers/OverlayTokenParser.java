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

import com.google.cloud.teleport.v2.neo4j.model.job.OverlayTokens;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OverlayTokenParser {

  private static final Logger LOG = LoggerFactory.getLogger(OverlayTokenParser.class);

  public static OverlayTokens parse(String optionsJson) {
    try {
      return new OverlayTokens(doParse(optionsJson));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Map<String, String> doParse(String json) {
    if (StringUtils.isBlank(json)) {
      return Map.of();
    }
    LOG.debug("Parsing overlay tokens: {}", json);
    var jsonToken = new JSONObject(json);
    var tokens = jsonToken.keys();
    var result = new HashMap<String, String>();
    while (tokens.hasNext()) {
      var tokenName = tokens.next();
      var rawTokenValue = jsonToken.opt(tokenName);
      var tokenValue = JSONObject.NULL.equals(rawTokenValue) ? null : String.valueOf(rawTokenValue);
      result.put(tokenName, tokenValue);
      LOG.debug("{}: {}", tokenName, tokenValue);
    }
    return result;
  }
}
