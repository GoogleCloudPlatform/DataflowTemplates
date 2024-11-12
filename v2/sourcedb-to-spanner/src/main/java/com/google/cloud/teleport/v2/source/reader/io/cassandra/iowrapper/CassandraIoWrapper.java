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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper;

import com.google.cloud.teleport.v2.source.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/** IOWrapper for Cassandra Source. */
public class CassandraIoWrapper implements IoWrapper {

  /** Get a list of reader transforms for Cassandra source. */
  @Override
  public ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>>
      getTableReaders() {
    // TODO(vardhanvthigle)
    return null;
  }

  /** Discover source schema for Cassandra. */
  @Override
  public SourceSchema discoverTableSchema() {
    // TODO(vardhanvthigle)
    return null;
  }
}
