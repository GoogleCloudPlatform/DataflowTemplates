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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Discover the Collation Mapping information for a given {@link CollationReference}. */
public class CollationMapperDoFn
    extends DoFn<CollationReference, KV<CollationReference, CollationMapper>>
    implements Serializable {

  private static final Logger logger = LoggerFactory.getLogger(CollationMapper.class);
  private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
  private final UniformSplitterDBAdapter dbAdapter;

  @JsonIgnore private transient @Nullable DataSource dataSource;

  public CollationMapperDoFn(
      SerializableFunction<Void, DataSource> dataSourceProviderFn,
      UniformSplitterDBAdapter dbAdapter) {
    this.dataSourceProviderFn = dataSourceProviderFn;
    this.dbAdapter = dbAdapter;
    this.dataSource = null;
  }

  @Setup
  public void setup() throws Exception {
    dataSource = dataSourceProviderFn.apply(null);
  }

  private Connection acquireConnection() throws SQLException {
    return checkStateNotNull(this.dataSource).getConnection();
  }

  @ProcessElement
  public void processElement(
      @Element CollationReference input,
      OutputReceiver<KV<CollationReference, CollationMapper>> out)
      throws SQLException {

    try (Connection conn = acquireConnection()) {
      CollationMapper mapper = CollationMapper.fromDB(conn, dbAdapter, input);
      out.output(KV.of(input, mapper));
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
}
