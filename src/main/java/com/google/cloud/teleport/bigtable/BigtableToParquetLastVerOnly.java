/*
 * Copyright (C) 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.bigtable;

import com.google.bigtable.v2.RowFilter;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * Dataflow pipeline that exports data from a Cloud Bigtable table to Parquet files in GCS.
 * Only the last cell version is taken
 */
public class BigtableToParquetLastVerOnly extends BigtableToParquet {


    /**
     * Main entry point for pipeline execution.
     *
     * @param args Command line arguments to the pipeline.
     */
    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        RowFilter filter = RowFilter.newBuilder()
                .setCellsPerColumnLimitFilter(1).build();

        PipelineResult result = run(options, filter);

        // Wait for pipeline to finish only if it is not constructing a template.
        if (options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
            result.waitUntilFinish();
        }
    }
}
