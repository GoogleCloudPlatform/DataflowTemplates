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
package com.google.cloud.teleport.v2.spanner.migrations.connection;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConnectionHelperRequestTest {
  @Test
  public void testGetters_returnConstructorArgumentsWithJdbcUrlPrefix() {
    List<Shard> shards =
        ImmutableList.of(new Shard("id", "host", "5432", "user", "pass", "db", null, null, null));
    ConnectionHelperRequest request =
        new ConnectionHelperRequest(
            shards, "key=val", 7, "org.postgresql.Driver", "SELECT 1", "jdbc:postgresql://");

    assertThat(request.getShards()).isSameInstanceAs(shards);
    assertThat(request.getProperties()).isEqualTo("key=val");
    assertThat(request.getMaxConnections()).isEqualTo(7);
    assertThat(request.getDriver()).isEqualTo("org.postgresql.Driver");
    assertThat(request.getConnectionInitQuery()).isEqualTo("SELECT 1");
    assertThat(request.getJdbcUrlPrefix()).isEqualTo("jdbc:postgresql://");
  }
}
