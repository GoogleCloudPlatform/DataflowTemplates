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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.DataSourceProvider;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Discover the Collation Mapping information for a given {@link CollationReference}. */
public class CollationMapperDoFn
    extends DoFn<KV<String, CollationReference>, KV<CollationReference, CollationMapper>>
    implements Serializable {

  private static final Logger logger = LoggerFactory.getLogger(CollationMapper.class);
  private final DataSourceProvider dataSourceProvider;
  private final UniformSplitterDBAdapter dbAdapter;
  private transient DataSourceManager dataSourceManager;

  @JsonIgnore private transient @Nullable DataSource dataSource;

  public CollationMapperDoFn(
      DataSourceProvider dataSourceProvider, UniformSplitterDBAdapter dbAdapter) {
    this.dataSourceProvider = dataSourceProvider;
    this.dbAdapter = dbAdapter;
    this.dataSource = null;
  }

  @StartBundle
  public void startBundle() throws Exception {
    this.dataSourceManager =
        DataSourceManagerImpl.builder().setDataSourceProvider(dataSourceProvider).build();
  }

  @ProcessElement
  public void processElement(
      @Element KV<String, CollationReference> input,
      OutputReceiver<KV<CollationReference, CollationMapper>> out)
      throws SQLException {
    DataSource dataSource = dataSourceManager.getDatasource(input.getKey());
    try (Connection conn = dataSource.getConnection()) {
      CollationMapper mapper = CollationMapper.fromDB(conn, dbAdapter, input.getValue());
      out.output(KV.of(input.getValue(), mapper));
    } catch (Exception e) {
      logger.error(
          "Exception: {} while generating collationMapper for dataSource: {}, collationReference: {}",
          e,
          dataSource,
          input);
      // Beam will re-try exceptions generation during pipeline-run phase.
      throw e;
    }
  }

  @FinishBundle
  public void finishBundle() throws Exception {
    cleanupDataSource();
  }

  @Teardown
  public void tearDown() throws Exception {
    cleanupDataSource();
  }

  void cleanupDataSource() {
    if (this.dataSourceManager != null) {
      this.dataSourceManager.closeAll();
      this.dataSourceManager = null;
    }
  }
}
