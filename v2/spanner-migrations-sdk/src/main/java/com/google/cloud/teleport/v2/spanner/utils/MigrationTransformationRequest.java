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
package com.google.cloud.teleport.v2.spanner.utils;

import java.util.Map;

public class MigrationTransformationRequest {
  private String tableName;
  private Map<String, Object> requestRow;
  private String shardId;
  private String eventType;

  public String getTableName() {
    return tableName;
  }

  public Map<String, Object> getRequestRow() {
    return requestRow;
  }

  public String getShardId() {
    return shardId;
  }

  public String getEventType() {
    return eventType;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setRequestRow(Map<String, Object> requestRow) {
    this.requestRow = requestRow;
  }

  public void setShardId(String shardId) {
    this.shardId = shardId;
  }

  public void setEventType(String eventType) {
    this.eventType = eventType;
  }

  public MigrationTransformationRequest(
      String tableName, Map<String, Object> requestRow, String shardId, String eventType) {
    this.tableName = tableName;
    this.requestRow = requestRow;
    this.shardId = shardId;
    this.eventType = eventType;
  }
}
