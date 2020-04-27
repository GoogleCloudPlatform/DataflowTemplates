/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.v2.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DataStream will provide a client to allow access
 * to the DataStream GCP Service.
 */
public class DataStream implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(DataStream.class);
  private static String projectId;

  public DataStream(String projectId) {
    this.projectId = projectId;
  }

  // TODO: Confrim the params make sense with DataStream team
  public Map<String, String> getObjectSchema(String streamName, String objectName) {
    // TODO: Implement.  Likely function layout:
    // String connProfileName = getSourceConnectionProfile(streamName);
    // String discoveryData = discoverObjectDetails(streamName, connProfileName, objectName);

    // TODO: Remove, only for mocking
    Map<String, String> objectSchema = new HashMap<String, String>();
    return objectSchema;
  }

  // TODO: Confrim the params make sense with DataStream team
  public List<String> getPrimaryKeys(String streamName, String objectName) {
    // TODO: Remove, only for mocking
    List<String> primaryKeys = new ArrayList<String>();
    return primaryKeys;
  }

  public List<String> getSortKeys(String streamName, String objectName) {
    // TODO: Remove, only for mocking
    List<String> sortKeys = new ArrayList<String>();
    return sortKeys;
  }
}

