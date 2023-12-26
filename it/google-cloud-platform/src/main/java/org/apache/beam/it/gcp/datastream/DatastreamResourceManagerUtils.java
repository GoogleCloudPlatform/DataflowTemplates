/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.it.gcp.datastream;

import static org.apache.beam.it.common.utils.ResourceManagerUtils.generateResourceId;

import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

public class DatastreamResourceManagerUtils {

  private static final int MAX_RESOURCE_ID_LENGTH = 60;
  private static final Pattern ILLEGAL_RESOURCE_ID_CHARS = Pattern.compile("[^a-zA-Z0-9- ]");
  private static final DateTimeFormatter TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSSSSS");

  private DatastreamResourceManagerUtils() {}

  /**
   * Utility function to generate a formatted resource ID.
   *
   * <p>A Datastream resource ID must contain only alphanumeric characters, underscores and spaces
   * with a max length of 60.
   *
   * @param resourceId the resource ID to be formatted into a valid ID.
   * @return a Datastream compatible resource ID.
   */
  static String generateDatastreamId(String resourceId) {
    return generateResourceId(
        resourceId, ILLEGAL_RESOURCE_ID_CHARS, "-", MAX_RESOURCE_ID_LENGTH, TIME_FORMAT);
  }
}
