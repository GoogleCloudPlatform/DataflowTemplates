package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.dto.SpannerTableReadConfiguration;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class SpannerReaderTransform extends PTransform<PCollection<SpannerTableReadConfiguration>, PCollection<Struct>> {

  private final SpannerConfig spannerConfig;

  public SpannerReaderTransform(SpannerConfig spannerConfig) {
    this.spannerConfig = spannerConfig;
  }

  @Override
  public PCollection<Struct> expand(PCollection<SpannerTableReadConfiguration> input) {
    return input.apply("CreateReadOps", ParDo.of(new DoFn<SpannerTableReadConfiguration, ReadOperation>() {
      @ProcessElement
      public void processElement(@Element SpannerTableReadConfiguration spannerTableReadConfiguration, OutputReceiver<ReadOperation> out) {
        String query = String.format("SELECT * FROM %s", spannerTableReadConfiguration.getTableName());
        out.output(ReadOperation.create().withQuery(query));
      }
    })).apply("ReadSpannerRecords", SpannerIO.readAll().withSpannerConfig(spannerConfig));
  }
}
