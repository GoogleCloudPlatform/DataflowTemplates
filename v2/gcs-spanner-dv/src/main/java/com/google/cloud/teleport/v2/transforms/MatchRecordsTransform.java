package com.google.cloud.teleport.v2.transforms;

import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MATCHED_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MISSING_IN_SOURCE_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MISSING_IN_SPANNER_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.SOURCE_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.SPANNER_TAG;

import com.google.cloud.teleport.v2.dofn.FunnelComparedRecordsFn;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.jetbrains.annotations.NotNull;

public class MatchRecordsTransform extends
    PTransform<@NotNull PCollectionTuple, @NotNull PCollectionTuple> {

  @Override
  public @NotNull PCollectionTuple expand(PCollectionTuple input) {
    PCollection<ComparisonRecord> sourceRecords = input.get(SOURCE_TAG);
    PCollection<ComparisonRecord> spannerRecords = input.get(SPANNER_TAG);

    PCollection<KV<String, ComparisonRecord>> sourceRecordsKv = sourceRecords
        .apply("MapSourceToKv", WithKeys.of(ComparisonRecord::getHash))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), sourceRecords.getCoder()));

    PCollection<KV<String, ComparisonRecord>> spannerRecordsKv = spannerRecords
        .apply("MapSpannerToKv", WithKeys.of(ComparisonRecord::getHash))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), spannerRecords.getCoder()));

    PCollection<KV<String, CoGbkResult>> coGbkResult = KeyedPCollectionTuple
        .of(SOURCE_TAG, sourceRecordsKv)
        .and(SPANNER_TAG, spannerRecordsKv)
        .apply("CoGroupByKey", CoGroupByKey.create());

    return coGbkResult.apply("CompareRecords", ParDo.of(
            new FunnelComparedRecordsFn())
        .withOutputTags(MATCHED_TAG,
            TupleTagList.of(MISSING_IN_SPANNER_TAG).and(MISSING_IN_SOURCE_TAG)));
  }
}
