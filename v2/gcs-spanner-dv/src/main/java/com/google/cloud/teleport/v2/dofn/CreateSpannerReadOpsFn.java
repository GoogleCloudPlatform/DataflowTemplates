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
package com.google.cloud.teleport.v2.dofn;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateSpannerReadOpsFn extends DoFn<Void, ReadOperation> {

  private static final Logger LOG = LoggerFactory.getLogger(CreateSpannerReadOpsFn.class);

  private final PCollectionView<Ddl> ddlView;
  private final SerializableFunction<Ddl, ISchemaMapper> schemaMapperProvider;
  private final com.google.cloud.teleport.v2.config.ValidationTableConfig tableConfig;

  public CreateSpannerReadOpsFn(
      PCollectionView<Ddl> ddlView,
      SerializableFunction<Ddl, ISchemaMapper> schemaMapperProvider,
      com.google.cloud.teleport.v2.config.ValidationTableConfig tableConfig) {
    this.ddlView = ddlView;
    this.schemaMapperProvider = schemaMapperProvider;
    this.tableConfig = tableConfig;
  }

  // TODO: @aasthabharill to check if there's a better way to generalize dialect specific changes
  @ProcessElement
  public void processElement(ProcessContext c) {
    Ddl ddl = c.sideInput(ddlView);
    ISchemaMapper schemaMapper = schemaMapperProvider.apply(ddl);
    List<String> tableNames = ddl.getTablesOrderedByReference();

    for (String tableName : tableNames) {
      if (!tableConfig.isSpannerTableAllowed(tableName, schemaMapper)) {
        LOG.info("Skipping Spanner table {} as it is not in the configured validation list.", tableName);
        continue;
      }
      String quote = ddl.dialect() == com.google.cloud.spanner.Dialect.POSTGRESQL ? "\"" : "`";
      // We encode the tableName in the query itself to push table information dynamically
      // and avoid table level stages.
      String query =
          String.format(
              "SELECT *, '%s' as __tableName__ FROM %s%s%s",
              tableName, quote, tableName, quote);
      c.output(ReadOperation.create().withQuery(query));
    }
  }
}
