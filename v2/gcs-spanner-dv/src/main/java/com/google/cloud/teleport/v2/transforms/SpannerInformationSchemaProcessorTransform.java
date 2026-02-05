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

import com.google.cloud.teleport.v2.dofn.ProcessInformationSchemaFn;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionView;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Beam transform which reads the information schema of the Spanner database and returns a Ddl
 * object.
 */
public class SpannerInformationSchemaProcessorTransform
    extends PTransform<@NotNull PBegin, @NotNull PCollectionView<Ddl>> {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerInformationSchemaProcessorTransform.class);

  private final SpannerConfig spannerConfig;

  public SpannerInformationSchemaProcessorTransform(SpannerConfig spannerConfig) {
    this.spannerConfig = spannerConfig;
  }

  @Override
  public @NotNull PCollectionView<Ddl> expand(PBegin p) {
    return p.apply("Pulse", Create.of((Void) null))
        .apply(
            "ReadSpannerInformationSchema", ParDo.of(new ProcessInformationSchemaFn(spannerConfig)))
        .apply("FetchDdlAsView", View.asSingleton());
  }
}
