package com.google.cloud.teleport.v2.dofn;

import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MATCHED_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MISSING_IN_SOURCE_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MISSING_IN_SPANNER_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.SOURCE_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.SPANNER_TAG;

import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;

public class FunnelComparedRecordsFn extends DoFn<KV<String, CoGbkResult>, ComparisonRecord> {

  @ProcessElement
  public void processElement(ProcessContext c) {
    KV<String, CoGbkResult> element = c.element();
    CoGbkResult result = element.getValue();

    Iterable<ComparisonRecord> sourceGroup = result.getAll(SOURCE_TAG);
    Iterable<ComparisonRecord> spannerGroup = result.getAll(SPANNER_TAG);

    boolean inSource = sourceGroup.iterator().hasNext();
    boolean inSpanner = spannerGroup.iterator().hasNext();

    // Match
    if (inSource && inSpanner) {
      // Output all matched records from source
      for (ComparisonRecord r : sourceGroup) {
        c.output(MATCHED_TAG, r);
      }
    }
    // Missing in Spanner (In Source ONLY)
    else if (inSource) {
      for (ComparisonRecord r : sourceGroup) {
        c.output(MISSING_IN_SPANNER_TAG, r);
      }
    }
    // Missing in Source (In Spanner ONLY)
    else if (inSpanner) {
      for (ComparisonRecord r : spannerGroup) {
        c.output(MISSING_IN_SOURCE_TAG, r);
      }
    }
  }
}
