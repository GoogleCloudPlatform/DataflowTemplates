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
package com.google.cloud.teleport.v2.elasticsearch.utils;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.NoSuchElementException;

/** Miscellaneous util methods for googlecloud-to-elasticsearch. */
public class ElasticsearchUtils {

  public static String getTimestampFromOriginalPayload(JsonNode node)
      throws NoSuchElementException {
    if (node.has("timestamp")) {
      return node.get("timestamp").asText();
    } else {
      if (node.has("protoPayload") && node.get("protoPayload").has("timestamp")) {
        return node.get("protoPayload").get("timestamp").asText();
      }
    }

    throw new NoSuchElementException("Unable to find \"timestamp\" value");
  }
}
