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
package com.google.cloud.teleport.v2.source.jdbc;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.ISourceConnector;
import com.google.cloud.teleport.v2.source.jdbc.iowrapper.JdbcIoWrapper;
import com.google.cloud.teleport.v2.source.jdbc.iowrapper.config.JdbcIoWrapperConfigGroup;
import com.google.cloud.teleport.v2.source.reader.io.IoWrapper;
import java.util.List;
import org.apache.beam.sdk.transforms.Wait;

/** Implementation for single instance JDBC sources. */
public class SingleInstanceJdbcDbConfigContainer implements JdbcDbConfigContainer {
  private final SourceDbToSpannerOptions options;
  private final ISourceConnector connector;

  public SingleInstanceJdbcDbConfigContainer(
      ISourceConnector connector, SourceDbToSpannerOptions options) {
    this.connector = connector;
    this.options = options;
  }

  @Override
  public JdbcIoWrapperConfigGroup getJdbcIoWrapperConfigGroup(
      List<String> sourceTables, Wait.OnSignal<?> waitOnSignal) {
    return JdbcIoWrapperConfigGroup.builder()
        .addShardConfig(
            OptionsToConfigBuilder.getJdbcIOWrapperConfigWithDefaults(
                connector, options, sourceTables, null, waitOnSignal))
        .build();
  }

  @Override
  public IoWrapper getIOWrapper(List<String> sourceTables, Wait.OnSignal<?> waitOnSignal) {
    return JdbcIoWrapper.of(getJdbcIoWrapperConfigGroup(sourceTables, waitOnSignal), connector);
  }
}
