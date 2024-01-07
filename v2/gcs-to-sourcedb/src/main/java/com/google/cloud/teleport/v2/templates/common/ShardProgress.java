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
package com.google.cloud.teleport.v2.templates.common;

import com.google.cloud.Timestamp;

/** Holds the shard progress details. */
public class ShardProgress {

  private String shard;
  private Timestamp fileStartInterval;
  private String status;

  public ShardProgress(String shard, Timestamp fileStartInterval, String status) {
    this.shard = shard;
    this.fileStartInterval = fileStartInterval;
    this.status = status;
  }

  public String getShard() {
    return shard;
  }

  public Timestamp getFileStartInterval() {
    return fileStartInterval;
  }

  public String getStatus() {
    return status;
  }
}
