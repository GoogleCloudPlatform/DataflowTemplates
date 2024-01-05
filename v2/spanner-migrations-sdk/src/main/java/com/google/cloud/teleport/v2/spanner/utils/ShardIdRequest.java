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
package com.google.cloud.teleport.v2.spanner.utils;

import java.util.Map;

/*
 * <h3>Reading Spanner Record from ShardIdRequest</h3>
 * <pre>{@code
 * String shardId = shardIdRequest.getSpannerRecord().get(shardIdColumn).toString();
 * Long integerColumn = (Long) shardIdRequest.getSpannerRecord().get(integerColumn);
 * }</pre>
 * <p> The get() in getSpannerRecord() takes spanner column name as input.</p>
 */
public class ShardIdRequest {

  private String tableName;
  private Map<String, Object> spannerRecord;

  public ShardIdRequest(String tableName, Map<String, Object> spannerRecord) {
    this.tableName = tableName;
    this.spannerRecord = spannerRecord;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public Map<String, Object> getSpannerRecord() {
    return spannerRecord;
  }

  public void setSpannerRecord(Map<String, Object> spannerRecord) {
    this.spannerRecord = spannerRecord;
  }
}
