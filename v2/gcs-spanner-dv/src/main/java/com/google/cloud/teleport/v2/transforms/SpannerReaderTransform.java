package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.v2.dofn.CreateSpannerReadOpsFn;
import com.google.cloud.teleport.v2.dofn.SpannerHashFn;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.jetbrains.annotations.NotNull;

public class SpannerReaderTransform extends
    PTransform<@NotNull PBegin, @NotNull PCollection<ComparisonRecord>> {

  private final SpannerConfig spannerConfig;

  private final PCollectionView<Ddl> ddlView;

  public SpannerReaderTransform(SpannerConfig spannerConfig, PCollectionView<Ddl> ddlView) {
    this.spannerConfig = spannerConfig;
    this.ddlView = ddlView;
  }

  @Override
  public @NotNull PCollection<ComparisonRecord> expand(PBegin p) {
    return p.apply("Pulse", Create.of((Void) null))
        .apply("CreateReadOps",
            ParDo.of(new CreateSpannerReadOpsFn(ddlView)).withSideInputs(ddlView)
        )
        .apply("ReadSpannerRecords", SpannerIO.readAll()
            .withSpannerConfig(spannerConfig)
            //we read from a snapshot ~15s ago to avoid locking, 15s is okay because
            //we expect batch validation to start >> 15s after bulk migration is finished.
            .withTimestampBound(TimestampBound.ofExactStaleness(15, TimeUnit.SECONDS))
            .withBatching(true)
        ).apply("ConvertSpannerRecordsToHash",
            ParDo.of(new SpannerHashFn(ddlView)).withSideInputs(ddlView));
  }
}
