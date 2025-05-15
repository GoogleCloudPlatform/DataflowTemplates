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
import com.google.cloud.teleport.v2.source.reader.io.cassandra.schema.CassandraSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.datasource.DataSource;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaDiscovery;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/** IOWrapper for Cassandra Source. */
public final class CassandraIoWrapper implements IoWrapper {
  private SourceSchema sourceSchema;
  private ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>>
      tableReaders;

  public CassandraIoWrapper(
      String gcsPath, List<String> sourceTables, @Nullable Integer numPartitions) {
    DataSource dataSource = CassandraIOWrapperHelper.buildDataSource(gcsPath, numPartitions);
    SchemaDiscovery schemaDiscovery = CassandraIOWrapperHelper.buildSchemaDiscovery();
    SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder()
                .setKeyspaceName(dataSource.cassandra().loggedKeySpace())
                .build());

    ImmutableList<String> tablesToRead =
        CassandraIOWrapperHelper.getTablesToRead(
            sourceTables, dataSource, schemaDiscovery, sourceSchemaReference);
    this.sourceSchema =
        CassandraIOWrapperHelper.getSourceSchema(
            schemaDiscovery, dataSource, sourceSchemaReference, tablesToRead);
    this.tableReaders = CassandraIOWrapperHelper.getTableReaders(dataSource, sourceSchema);
  }

  /** Get a list of reader transforms for Cassandra source. */
  @Override
  public ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>>
      getTableReaders() {
    return tableReaders;
  }

  /** Discover source schema for Cassandra. */
  @Override
  public SourceSchema discoverTableSchema() {
    return sourceSchema;
  }
}
