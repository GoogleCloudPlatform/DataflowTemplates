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
package com.google.cloud.teleport.v2.source.reader.io;

import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/** IO Wrapper Interface for adding new IO sources. */
public interface IoWrapper {

  /** Get a list of reader transforms. */
  /**
   * Returns a mapping of table reference groups to their corresponding reader transforms.
   *
   * <p>For legacy single-table reads, the list will contain a single {@link SourceTableReference}.
   * For optimized multi-table reads, the list will contain all table references handled by that
   * specific transform.
   *
   * <p>Note: The {@link SourceRow} generics are handled by the caller. Ensure that the PCollection
   * produced by the transform is compatible with the expected pipeline schema to avoid type erasure
   * issues in Beam.
   *
   * @return A map where the key is a list of tables and the value is the transform to read them.
   */
  ImmutableMap<ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
      getTableReaders();

  /**
   * Discover source schema.
   *
   * @return List of discovered schemas for all the passed sources. For single shard migration the
   *     returned list will contain a single element. Note that datasources like Cassandra support
   *     only single shard migration.
   */
  ImmutableList<SourceSchema> discoverTableSchema();
}
