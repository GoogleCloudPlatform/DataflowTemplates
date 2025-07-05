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
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KafkaTopicUtils {

  private static final Pattern GMK_PATTERN =
      Pattern.compile("^projects/([^/]+)/locations/([^/]+)/clusters/([^/]+)/topics/([^/]+)$");

  public static List<String> getBootstrapServerAndTopic(
      String bootstrapServerAndTopicString, String project) {
    Matcher matcher = GMK_PATTERN.matcher(bootstrapServerAndTopicString);
    if (matcher.matches()) {
      String bootstrapServer =
          "bootstrap."
              + matcher.group(3)
              + "."
              + matcher.group(2)
              + ".managedkafka."
              + project
              + ".cloud.goog:9092";
      String topicName = matcher.group(4);
      return List.of(bootstrapServer, topicName);
    }
    String[] list = bootstrapServerAndTopicString.split(";");
    String bootstrapServer = list[0];
    String[] topicNames = list[1].split(",");
    return Stream.concat(Stream.of(bootstrapServer), Stream.of(topicNames))
        .collect(Collectors.toList());
  }
}
