/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.io.CdcJdbcIO.DataSourceConfiguration;
import com.google.cloud.teleport.v2.utils.DatastreamToDML;
import com.google.cloud.teleport.v2.utils.DatastreamToMySQLDML;
import com.google.cloud.teleport.v2.utils.DatastreamToPostgresDML;
import com.google.cloud.teleport.v2.values.DmlInfo;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code CreateDml} class batches data to ensure connection limits and builds the {@link
 * DmlInfo} objects.
 */
public class CreateDml
    extends PTransform<
        PCollection<FailsafeElement<String, String>>, PCollection<KV<String, DmlInfo>>> {

  private static final Logger LOG = LoggerFactory.getLogger(CreateDml.class);
  private static final String WINDOW_DURATION = "1s";
  private static final Integer NUM_THREADS = new Integer(100);
  private static DataSourceConfiguration dataSourceConfiguration;
  private static Map<String, String> schemaMap = new HashMap<String, String>();

  private CreateDml(DataSourceConfiguration dataSourceConfiguration) {
    this.dataSourceConfiguration = dataSourceConfiguration;
  }

  public static CreateDml of(DataSourceConfiguration dataSourceConfiguration) {
    return new CreateDml(dataSourceConfiguration);
  }

  public CreateDml withSchemaMap(Map<String, String> schemaMap) {
    this.schemaMap = schemaMap;
    return this;
  }

  public DatastreamToDML getDatastreamToDML() {
    DatastreamToDML datastreamToDML;
    String driverName = this.dataSourceConfiguration.getDriverClassName().get();
    switch (driverName) {
      case "org.postgresql.Driver":
        datastreamToDML = DatastreamToPostgresDML.of(dataSourceConfiguration);
        break;
      case "com.mysql.cj.jdbc.Driver":
        datastreamToDML = DatastreamToMySQLDML.of(dataSourceConfiguration);
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Database Driver %s is not supported.", driverName));
    }

    return datastreamToDML.withSchemaMap(schemaMap);
  }

  @Override
  public PCollection<KV<String, DmlInfo>> expand(
      PCollection<FailsafeElement<String, String>> input) {
    DatastreamToDML datastreamToDML = getDatastreamToDML();
    return input
        .apply(
            "Reshuffle Into Buckets",
            Reshuffle.<FailsafeElement<String, String>>viaRandomKey().withNumBuckets(NUM_THREADS))
        .apply("Format to Postgres DML", ParDo.of(datastreamToDML));
  }
}
