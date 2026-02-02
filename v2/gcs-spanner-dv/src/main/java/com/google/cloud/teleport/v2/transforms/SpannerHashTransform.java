package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.dofn.SpannerHashFn;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

public class SpannerHashTransform extends PTransform<PCollection<Struct>, PCollection<ComparisonRecord>> {

  private final PCollectionView<Ddl> ddlView;

  public SpannerHashTransform(PCollectionView<Ddl> ddlView) {
    this.ddlView = ddlView;
  }

  @Override
  public PCollection<ComparisonRecord> expand(PCollection<Struct> spannerRecords) {
    return spannerRecords.apply("ConvertSpannerRecordsToHash",
        ParDo.of(new SpannerHashFn(ddlView)).withSideInputs(ddlView));
  }
}
