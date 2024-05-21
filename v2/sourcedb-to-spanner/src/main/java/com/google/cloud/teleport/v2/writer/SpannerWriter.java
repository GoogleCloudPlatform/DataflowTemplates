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
package com.google.cloud.teleport.v2.writer;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.templates.RowContext;
import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.FailureMode;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.Write;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Write the rows coming in from the transformer to Spanner efficiently. */
public class SpannerWriter implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerWriter.class);

  private final SpannerConfig spannerConfig;

  public SpannerWriter(SpannerConfig spannerConfig) {
    this.spannerConfig = spannerConfig;
  }

  public Write getSpannerWrite() {
    return SpannerIO.write()
        .withSpannerConfig(spannerConfig)
        .withFailureMode(FailureMode.REPORT_FAILURES);
  }

  public PCollection<MutationGroup> writeToSpanner(PCollection<RowContext> rows) {
    LOG.info("initiating write to spanner");
    SpannerWriteResult writeResult =
        rows.apply(
                "extractMutation",
                MapElements.into(TypeDescriptor.of(Mutation.class))
                    .via((RowContext r) -> r.mutation()))
            .apply("WriteToSpanner", getSpannerWrite());

    // This current returns only the failed mutation.
    // This needs to return the whole RowContext and Exception
    return writeResult.getFailedMutations();
  }
}
