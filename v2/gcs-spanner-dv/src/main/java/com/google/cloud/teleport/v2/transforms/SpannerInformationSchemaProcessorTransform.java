package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.dofn.ProcessInformationSchemaFn;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Beam transform which reads the information schema of the Spanner database and returns
 * a Ddl object.
 */
public class SpannerInformationSchemaProcessorTransform
    extends PTransform<PBegin, PCollection<Ddl>> {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerInformationSchemaProcessorTransform.class);



  private final SpannerConfig spannerConfig;

  public SpannerInformationSchemaProcessorTransform(
      SpannerConfig spannerConfig) {
    this.spannerConfig = spannerConfig;
  }

  @Override
  public PCollection<Ddl> expand(PBegin p) {
    return p.apply("Pulse", Create.of((Void) null))
        .apply(
            "ReadSpannerInformationSchema",
            ParDo.of(
                new ProcessInformationSchemaFn(
                    spannerConfig)));
  }
}

