/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.templates.common;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Transforms & DoFns & Options for Teleport BigtableIO.
 */
public class BigtableConverters {

  /**
   * Options for writing Bigtable rows.
   */
  public interface BigtableWriteOptions extends PipelineOptions {

    @Description("GCP Project Id of where to write the Bigtable rows")
    ValueProvider<String> getBigtableWriteProjectId();

    void setBigtableWriteProjectId(ValueProvider<String> bigtableWriteProjectId);

    @Description("Bigtable Instance id")
    ValueProvider<String> getBigtableWriteInstanceId();

    void setBigtableWriteInstanceId(ValueProvider<String> bigtableWriteInstanceId);

    @Description("Bigtable table id")
    ValueProvider<String> getBigtableWriteTableId();

    void setBigtableWriteTableId(ValueProvider<String> bigtableWriteTableId);

    @Description("Bigtable column family name")
    ValueProvider<String> getBigtableWriteColumnFamily();

    void setBigtableWriteColumnFamily(ValueProvider<String> bigtableWriteColumnFamily);
  }

}
