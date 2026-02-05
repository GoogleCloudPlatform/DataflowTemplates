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
package com.google.cloud.teleport.v2.transforms;

import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MATCHED_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MISSING_IN_SOURCE_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MISSING_IN_SPANNER_TAG;

import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that takes a {@link PCollectionTuple} of {@link ComparisonRecord}s and logs
 * the counts of matched, missing in source, and missing in Spanner records.
 */
public class ReportResultsTransform extends PTransform<PCollectionTuple, PDone> {

  private static final Logger LOG = LoggerFactory.getLogger(ReportResultsTransform.class);

  @Override
  public PDone expand(PCollectionTuple input) {
    input
        .get(MATCHED_TAG)
        .apply(
            "ExtractTableNameMatched",
            MapElements.into(TypeDescriptors.strings()).via(ComparisonRecord::getTableName))
        .apply("CountMatched", Count.perElement())
        .apply("LogMatched", ParDo.of(new LogCountFn("Matched")));

    input
        .get(MISSING_IN_SPANNER_TAG)
        .apply(
            "ExtractTableNameMissingInSpanner",
            MapElements.into(TypeDescriptors.strings()).via(ComparisonRecord::getTableName))
        .apply("CountMissingInSpanner", Count.perElement())
        .apply("LogMissingInSpanner", ParDo.of(new LogCountFn("MissingInSpanner")));

    input
        .get(MISSING_IN_SOURCE_TAG)
        .apply(
            "ExtractTableNameMissingInSource",
            MapElements.into(TypeDescriptors.strings()).via(ComparisonRecord::getTableName))
        .apply("CountMissingInSource", Count.perElement())
        .apply("LogMissingInSource", ParDo.of(new LogCountFn("MissingInSource")));

    return PDone.in(input.getPipeline());
  }

  @VisibleForTesting
  static class LogCountFn extends DoFn<KV<String, Long>, Void> {

    private final String label;

    public LogCountFn(String label) {
      this.label = label;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      LOG.info("{}: {} - {}", label, c.element().getKey(), c.element().getValue());
    }
  }
}
