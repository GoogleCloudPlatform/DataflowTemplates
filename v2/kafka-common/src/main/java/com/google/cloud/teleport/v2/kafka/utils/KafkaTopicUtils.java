/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.kafka.utils;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaTopicUtils {

  private static final Pattern GMK_PATTERN =
      Pattern.compile("^projects/([^/]+)/locations/([^/]+)/clusters/([^/]+)/topics/([^/]+)$");

  private static String sanitizeProjectIdForHostname(String projectId) {
    // Domain-scoped project IDs (like 'example.com:my-project') map to
    // 'my-project.example.com' in cloud.goog hostnames.
    // ':' is not valid in hostnames, but '.' is allowed as a label separator.
    // Normalize to lowercase and replace any character not [a-z0-9-.] with '-'.
    String normalized = projectId.toLowerCase();
    if (normalized.contains(":")) {
      String[] parts = normalized.split(":", 2);
      String domain = parts[0];
      String proj = parts[1];
      normalized = proj + "." + domain;
    }
    return normalized.replaceAll("[^a-z0-9-.]", "-");
  }

  public static List<String> getBootstrapServerAndTopic(
      String bootstrapServerAndTopicString, String project) {
    Matcher matcher = GMK_PATTERN.matcher(bootstrapServerAndTopicString);
    String bootstrapServer = null;
    String topicName = null;
    if (matcher.matches()) {
      String clusterId = matcher.group(3);
      String region = matcher.group(2);
      String resourceProjectId = sanitizeProjectIdForHostname(matcher.group(1));
      bootstrapServer =
          "bootstrap."
              + clusterId
              + "."
              + region
              + ".managedkafka."
              + resourceProjectId
              + ".cloud.goog:9092";
      topicName = matcher.group(4);
    } else {
      String[] list = bootstrapServerAndTopicString.split(";");
      bootstrapServer = list[0];
      topicName = list[1];
    }
    return List.of(bootstrapServer, topicName);
  }
}
