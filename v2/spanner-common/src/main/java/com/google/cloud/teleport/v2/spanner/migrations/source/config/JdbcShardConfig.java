/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.spanner.migrations.source.config;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import java.util.List;

/**
 * Represents the connection configuration for a sharded RDBMS source (MySQL/PG).
 *
 * <p>For non-sharded RDBMS, shardConfigs will contain single element.
 */
public class JdbcShardConfig implements SourceConnectionConfig {
  private List<Shard> shardConfigs;

  public List<Shard> getShardConfigs() {
    return shardConfigs;
  }

  public void setShardConfigs(List<Shard> shardConfigs) {
    this.shardConfigs = shardConfigs;
  }
}
