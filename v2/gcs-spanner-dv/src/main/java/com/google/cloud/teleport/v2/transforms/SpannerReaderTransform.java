package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.dofn.CreateSpannerReadOpsFn;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.jetbrains.annotations.NotNull;

public class SpannerReaderTransform extends
    PTransform<@NotNull PBegin, @NotNull PCollection<Struct>> {

  private final SpannerConfig spannerConfig;

  private final PCollectionView<Ddl> ddlView;

  public SpannerReaderTransform(SpannerConfig spannerConfig, PCollectionView<Ddl> ddlView) {
    this.spannerConfig = spannerConfig;
    this.ddlView = ddlView;
  }

  @Override
  public @NotNull PCollection<Struct> expand(PBegin p) {
    return p.apply("Pulse", Create.of((Void) null))
        .apply("CreateReadOps",
            ParDo.of(new CreateSpannerReadOpsFn(ddlView)).withSideInputs(ddlView)
        )
        .apply("DistributeSpannerReadOps", Redistribute.arbitrarily())
        .apply("ReadSpannerRecords", SpannerIO.readAll().withSpannerConfig(spannerConfig));
  }
}
