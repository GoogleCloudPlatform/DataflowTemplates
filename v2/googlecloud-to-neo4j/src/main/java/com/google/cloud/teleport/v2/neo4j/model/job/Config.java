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
package com.google.cloud.teleport.v2.neo4j.model.job;

import com.google.cloud.teleport.v2.neo4j.model.enums.AvroType;
import java.io.Serializable;
import org.json.JSONObject;

/** Global configuration options. */
public class Config implements Serializable {

  public Boolean resetDb = false;
  public Boolean indexAllProperties = false;

  public String auditGsUri;
  public AvroType avroType = AvroType.parquet;

  public Integer nodeParallelism = 5;
  public Integer edgeParallelism = 1;
  public Integer nodeBatchSize = 5000;
  public Integer edgeBatchSize = 1000;

  public Config() {}

  public Config(JSONObject jsonObject) {
    resetDb = jsonObject.has("reset_db") && jsonObject.getBoolean("reset_db");
    auditGsUri =
        jsonObject.has("audit_gcs_uri") ? jsonObject.getString("audit_gcs_uri") : auditGsUri;
    nodeParallelism =
        jsonObject.has("node_write_batch_size")
            ? jsonObject.getInt("node_write_batch_size")
            : nodeParallelism;
    edgeParallelism =
        jsonObject.has("edge_write_batch_size")
            ? jsonObject.getInt("edge_write_batch_size")
            : edgeParallelism;
    // not currently implemented
    nodeBatchSize =
        jsonObject.has("node_write_parallelism")
            ? jsonObject.getInt("node_write_parallelism")
            : nodeBatchSize;
    edgeBatchSize =
        jsonObject.has("edge_write_parallelism")
            ? jsonObject.getInt("edge_write_parallelism")
            : edgeBatchSize;
    indexAllProperties =
        jsonObject.has("index_all_properties")
            ? jsonObject.getBoolean("index_all_properties")
            : indexAllProperties;
  }
}
