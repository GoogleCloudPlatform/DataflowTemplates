/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.pubsub;

import java.io.Serializable;

/** Holds the PubSub Consumer profile. */
public class PubSubConsumerProfile implements Serializable {

  private int maxReadMessageCount;
  private String projectId;

  public PubSubConsumerProfile(String projectId, int maxReadMessageCount) {
    this.projectId = projectId;
    this.maxReadMessageCount = maxReadMessageCount;
  }

  public String getProjectId() {
    return projectId;
  }

  public int getMaxReadMessageCount() {
    return maxReadMessageCount;
  }

  @Override
  public String toString() {
    return " { projectId: " + projectId + " , maxReadMessageCount: " + maxReadMessageCount + "}";
  }
}
