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
package com.google.cloud.teleport.v2.templates.model;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MySqlSinkConfigTest {

  @Test
  public void testMySqlSinkConfig_gettersSettersAndConstructors() {
    MySqlSinkConfig config1 = new MySqlSinkConfig();
    List<Shard> shards = new ArrayList<>();
    Shard shard = new Shard("shard1", "host", "3306", "user", "pwd", "db", null, null, "");
    shards.add(shard);
    config1.setShards(shards);

    assertThat(config1.getShards()).hasSize(1);
    assertThat(config1.getShards().get(0).getLogicalShardId()).isEqualTo("shard1");

    MySqlSinkConfig config2 = new MySqlSinkConfig(shards);
    assertThat(config2).isEqualTo(config1);
    assertThat(config2.hashCode()).isEqualTo(config1.hashCode());
    assertThat(config2.toString()).contains("shard1");

    MySqlSinkConfig config3 = new MySqlSinkConfig(new ArrayList<>());
    assertThat(config3).isNotEqualTo(config1);
    assertThat(config1).isNotEqualTo(null);
    assertThat(config1).isNotEqualTo("some-string");
  }
}
