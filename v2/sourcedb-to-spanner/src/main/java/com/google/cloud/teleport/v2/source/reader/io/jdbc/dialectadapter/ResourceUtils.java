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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceUtils {
  private static final Logger logger = LoggerFactory.getLogger(ResourceUtils.class);

  public static final String CHARSET_REPLACEMENT_TAG = "charset_replacement_tag";
  public static final String COLLATION_REPLACEMENT_TAG = "collation_replacement_tag";

  /**
   * Load a resource file as string.
   *
   * @param resource path of the resource file.
   * @return resource file as string.
   */
  public static String resourceAsString(String resource) {
    try {
      URL url = com.google.common.io.Resources.getResource(resource);
      return com.google.common.io.Resources.toString(url, StandardCharsets.UTF_8);
    } catch (Exception e) {
      // This exception should not happen in production as it really means we don't have the
      // expected resource file in bundled in the build or a fatal IO failure in reading one.
      logger.error(
          "Exception {} while trying to load the SQL file {} for collation discovery.",
          e,
          resource);
      throw new RuntimeException(e);
    }
  }

  /**
   * Replace the given tags in the query and sanitizes the query by removing blank lines and
   * comments.
   *
   * @param query query to prepare.
   * @param tags tag keys to be replaced by the tag values.
   * @return sanitized query with replaced tags
   */
  public static String replaceTagsAndSanitize(String query, Map<String, String> tags) {
    String processedQuery = query;
    // Replace tags
    for (Map.Entry<String, String> tag : tags.entrySet()) {
      processedQuery = processedQuery.replace(tag.getKey(), tag.getValue());
    }

    // Remove comments and blank lines.
    // Note we don't remove block comments for sake of simplicity.
    Pattern commentPattern = Pattern.compile("(?m)^[ \\t]?--.*$");
    Matcher matcher = commentPattern.matcher(processedQuery);
    processedQuery = matcher.replaceAll("");

    // Remove blank lines
    processedQuery = processedQuery.replaceAll("(?m)^\\s*\\r?\\n", "");
    return processedQuery;
  }
}
