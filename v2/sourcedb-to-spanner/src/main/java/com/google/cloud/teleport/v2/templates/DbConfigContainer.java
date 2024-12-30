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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.source.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.Wait;

/**
 * Container for DB Configuration. Implementations should encapsulate the relevant source
 * configuration to help with invoking sharded or non-sharded migration.
 */
public interface DbConfigContainer {

  /**
   * Get a Unique id for the physical data source. For Non-sharded migration, the id can be returned
   * as null.
   *
   * @return Unique id.
   */
  @Nullable
  String getShardId();

  /**
   * For the spanner tables that contain the shard id column, returns the source table to
   * shardColumn. For non-Sharded Migration, return empty Map.
   *
   * @param schemaMapper
   * @param spannerTables
   * @return
   */
  Map<String, String> getSrcTableToShardIdColumnMap(
      ISchemaMapper schemaMapper, List<String> spannerTables);

  /**
   * Create an {@link IoWrapper} instance for a list of SourceTables.
   *
   * @param sourceTables
   * @param waitOnSignal
   * @return
   */
  IoWrapper getIOWrapper(List<String> sourceTables, Wait.OnSignal<?> waitOnSignal);
}
