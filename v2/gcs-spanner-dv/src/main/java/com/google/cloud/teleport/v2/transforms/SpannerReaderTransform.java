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

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.v2.dofn.CreateSpannerReadOpsFn;
import com.google.cloud.teleport.v2.dofn.SpannerHashFn;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.jetbrains.annotations.NotNull;

public class SpannerReaderTransform
    extends PTransform<@NotNull PBegin, @NotNull PCollection<ComparisonRecord>> {

  private final SpannerConfig spannerConfig;

  private final PCollectionView<Ddl> ddlView;
  private final SerializableFunction<Ddl, ISchemaMapper> schemaMapperProvider;

  public SpannerReaderTransform(
      SpannerConfig spannerConfig,
      PCollectionView<Ddl> ddlView,
      SerializableFunction<Ddl, ISchemaMapper> schemaMapperProvider) {
    this.spannerConfig = spannerConfig;
    this.ddlView = ddlView;
    this.schemaMapperProvider = schemaMapperProvider;
  }

  @Override
  public @NotNull PCollection<ComparisonRecord> expand(PBegin p) {
    return p.apply("Pulse", Create.of((Void) null))
        .apply(
            "CreateReadOps", ParDo.of(new CreateSpannerReadOpsFn(ddlView)).withSideInputs(ddlView))
        .apply("ReadSpannerRecords", readFromSpanner())
        .apply(
            "CalculateSpannerRecordsHash",
            ParDo.of(new SpannerHashFn(ddlView, schemaMapperProvider)).withSideInputs(ddlView));
  }

  /**
   * Returns a PTransform that reads data from Spanner. Note: This method is extracted to enable
   * unit testing.
   */
  @VisibleForTesting
  protected PTransform<@NotNull PCollection<ReadOperation>, @NotNull PCollection<Struct>>
      readFromSpanner() {
    return SpannerIO.readAll()
        .withSpannerConfig(spannerConfig)
        // we read from a snapshot ~15s ago to avoid locking, 15s is okay because
        // we expect batch validation to start >> 15s after bulk migration is finished.
        .withTimestampBound(TimestampBound.ofExactStaleness(15, TimeUnit.SECONDS))
        .withBatching(true);
  }
}
