package com.google.cloud.teleport.v2.constants;

import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import org.apache.beam.sdk.values.TupleTag;

public class GCSSpannerDVConstants {

  public static final TupleTag<ComparisonRecord> SOURCE_TAG = new TupleTag<ComparisonRecord>() {
  };
  public static final TupleTag<ComparisonRecord> SPANNER_TAG = new TupleTag<ComparisonRecord>() {
  };
  public static final TupleTag<ComparisonRecord> MATCHED_TAG = new TupleTag<ComparisonRecord>() {
  };
  public static final TupleTag<ComparisonRecord> MISSING_IN_SPANNER_TAG = new TupleTag<ComparisonRecord>() {
  };
  public static final TupleTag<ComparisonRecord> MISSING_IN_SOURCE_TAG = new TupleTag<ComparisonRecord>() {
  };

}
