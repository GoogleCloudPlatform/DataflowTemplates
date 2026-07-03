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

import com.google.cloud.teleport.v2.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.JdbcIoWrapper;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.JdbcIoWrapperConfigGroup;
import com.google.cloud.teleport.v2.templates.DbConfigContainer;
import java.util.List;
import org.apache.beam.sdk.transforms.Wait;

/**
 * Interface for managing database configurations.
 *
 * <p>JDBC specific refinement of {@link DbConfigContainer} that returns {@link JdbcIoWrapper}.
 */
public interface JdbcDbConfigContainer extends DbConfigContainer {

  /**
   * Get the {@link JdbcIoWrapperConfigGroup} for the given source tables and wait signal.
   *
   * <p><b>Graph Size Optimization:</b> By returning a config group, we allow the {@link
   * JdbcIoWrapper} to aggregate multiple tables into a single or few reader transforms. This
   * effectively decouples the Dataflow graph size from the total number of tables being migrated.
   *
   * @param sourceTables List of source tables to migrate.
   * @param waitOnSignal Signal to wait on before starting the read.
   * @return {@link JdbcIoWrapperConfigGroup}
   */
  JdbcIoWrapperConfigGroup getJdbcIoWrapperConfigGroup(
      List<String> sourceTables, Wait.OnSignal<?> waitOnSignal);

  /**
   * Creates an {@link IoWrapper} for the given tables.
   *
   * <p><b>Backward Compatibility:</b> For single-shard migrations, the group will contain only one
   * shard config, and the resulting IO wrapper will behave identically to the previous
   * single-source implementation.
   */
  @Override
  default IoWrapper getIOWrapper(List<String> sourceTables, Wait.OnSignal<?> waitOnSignal) {
    return JdbcIoWrapper.of(getJdbcIoWrapperConfigGroup(sourceTables, waitOnSignal));
  }
}
