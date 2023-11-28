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

import com.google.cloud.teleport.v2.datastream.io.CdcJdbcIO;
import com.google.cloud.teleport.v2.datastream.values.DmlInfo;
import com.google.cloud.teleport.v2.utils.DatabaseMigrationUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code CreateDml} class batches data to ensure connection limits and builds the DmlInfo
 * objects.
 */
public class CreateDml {

  private static final Logger LOG = LoggerFactory.getLogger(CreateDml.class);
  private static final String WINDOW_DURATION = "1s";
  private static final Integer NUM_THREADS = new Integer(100);

  public CreateDml() {}

  public static CreateDmlFromRecord createDmlObjects(
      CdcJdbcIO.DataSourceConfiguration dataSourceConfiguration) {
    return new CreateDmlFromRecord(dataSourceConfiguration);
  }

  /**
   * This class is used as the default return value of {@link
   * CreateDml#createDmlObjects(DataSourceConfiguration)}}.
   */
  public static class CreateDmlFromRecord
      extends PTransform<
          PCollection<FailsafeElement<String, String>>, PCollection<KV<String, DmlInfo>>> {

    private static CdcJdbcIO.DataSourceConfiguration dataSourceConfiguration;

    public CreateDmlFromRecord(CdcJdbcIO.DataSourceConfiguration dataSourceConfiguration) {
      this.dataSourceConfiguration = dataSourceConfiguration;
    }

    @Override
    public PCollection<KV<String, DmlInfo>> expand(
        PCollection<FailsafeElement<String, String>> input) {
      DatabaseMigrationUtils databaseMigrationUtils =
          DatabaseMigrationUtils.of(dataSourceConfiguration);
      return input
          .apply(
              "Reshuffle Into Buckets",
              Reshuffle.<FailsafeElement<String, String>>viaRandomKey().withNumBuckets(NUM_THREADS))
          .apply(
              "Format to Postgres DML",
              MapElements.into(
                      TypeDescriptors.kvs(
                          TypeDescriptors.strings(), TypeDescriptor.of(DmlInfo.class)))
                  .via(element -> databaseMigrationUtils.convertJsonToDmlInfo(element)));
    }
  }
}
