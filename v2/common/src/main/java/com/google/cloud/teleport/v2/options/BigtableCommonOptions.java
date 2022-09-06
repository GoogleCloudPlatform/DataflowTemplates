/*
 * Copyright (C) 2020 Google LLC
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
package com.google.cloud.teleport.v2.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * Common {@link PipelineOptions} for writing data to Bigtable using {@link
 * com.google.cloud.bigtable.beam.CloudBigtableIO}.
 */
public final class BigtableCommonOptions {

  private BigtableCommonOptions() {}

  /** Provides {@link PipelineOptions} to write records to a Bigtable's table. */
  public interface WriteOptions extends PipelineOptions {

    @Description("GCP Project Id of where to write the Bigtable rows")
    @Required
    String getBigtableWriteProjectId();

    void setBigtableWriteProjectId(String value);

    @Description("Bigtable Instance id")
    @Required
    String getBigtableWriteInstanceId();

    void setBigtableWriteInstanceId(String value);

    @Description("Bigtable table id")
    @Required
    String getBigtableWriteTableId();

    void setBigtableWriteTableId(String value);

    @Description("Bigtable app profile")
    @Default.String("default")
    String getBigtableWriteAppProfile();

    void setBigtableWriteAppProfile(String value);
  }
}
