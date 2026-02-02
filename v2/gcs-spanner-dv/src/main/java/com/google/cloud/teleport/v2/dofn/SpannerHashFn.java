package com.google.cloud.teleport.v2.dofn;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

public class SpannerHashFn extends DoFn<Struct, ComparisonRecord> {

  private final PCollectionView<Ddl> ddlView;

  public SpannerHashFn(PCollectionView<Ddl> ddlView) {
    this.ddlView = ddlView;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    Ddl ddl = c.sideInput(ddlView);
    c.output(ComparisonRecord.fromSpannerStruct(c.element()));
  }
}
