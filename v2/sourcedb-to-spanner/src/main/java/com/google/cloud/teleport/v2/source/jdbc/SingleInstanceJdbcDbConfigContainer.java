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
package com.google.cloud.teleport.v2.source.jdbc;

import com.google.cloud.teleport.v2.options.OptionsToConfigBuilder;
import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.JdbcIoWrapperConfigGroup;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import java.util.List;
import org.apache.beam.sdk.transforms.Wait;

/** Implementation of {@link JdbcDbConfigContainer} for single instance JDBC migration. */
public class SingleInstanceJdbcDbConfigContainer implements JdbcDbConfigContainer {
  private SourceDbToSpannerOptions options;
  private Shard shard;

  public SingleInstanceJdbcDbConfigContainer(SourceDbToSpannerOptions options, Shard shard) {
    this.options = options;
    this.shard = shard;
  }

  @Override
  public JdbcIoWrapperConfigGroup getJdbcIoWrapperConfigGroup(
      List<String> sourceTables, Wait.OnSignal<?> waitOnSignal) {
    return JdbcIoWrapperConfigGroup.builder()
        .addShardConfig(
            OptionsToConfigBuilder.getJdbcIOWrapperConfigWithDefaults(
                options, shard, sourceTables, waitOnSignal))
        .build();
  }
}
