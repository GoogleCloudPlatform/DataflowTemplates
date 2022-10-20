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
import lombok.Getter;
import lombok.Setter;
import org.json.JSONObject;

/** Global configuration options. */
@Getter
@Setter
public class Config implements Serializable {

  private Boolean resetDb = false;
  private Boolean indexAllProperties = false;

  private AvroType avroType = AvroType.parquet;

  private Integer nodeParallelism = 5;
  private Integer edgeParallelism = 1;
  private Integer nodeBatchSize = 5000;
  private Integer edgeBatchSize = 1000;

  public Config() {}

  public Config(JSONObject jsonObject) {
    resetDb = jsonObject.has("reset_db") && jsonObject.getBoolean("reset_db");
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
