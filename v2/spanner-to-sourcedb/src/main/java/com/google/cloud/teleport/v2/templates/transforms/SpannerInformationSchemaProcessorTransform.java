/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.transforms;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Beam transform which 1) Reads information schema from both main and shadow table database. 2)
 * Create shadow tables in the shadow table database 3) Outputs both DDL schemas
 */
public class SpannerInformationSchemaProcessorTransform
    extends PTransform<PBegin, PCollectionTuple> {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerInformationSchemaProcessorTransform.class);

  public static final TupleTag<Ddl> MAIN_DDL_TAG = new TupleTag<>() {};
  public static final TupleTag<Ddl> SHADOW_TABLE_DDL_TAG = new TupleTag<>() {};

  private final SpannerConfig spannerConfig;
  private final SpannerConfig shadowTableSpannerConfig;
  private final String shadowTablePrefix;

  public SpannerInformationSchemaProcessorTransform(
      SpannerConfig spannerConfig,
      SpannerConfig shadowTableSpannerConfig,
      String shadowTablePrefix) {
    this.spannerConfig = spannerConfig;
    this.shadowTableSpannerConfig = shadowTableSpannerConfig;
    this.shadowTablePrefix = shadowTablePrefix;
  }

  @Override
  public PCollectionTuple expand(PBegin p) {
    return p.apply("Pulse", Create.of((Void) null))
        .apply(
            "Create Shadow tables and return Information Schemas",
            ParDo.of(
                    new ProcessInformationSchemaFn(
                        spannerConfig, shadowTableSpannerConfig, shadowTablePrefix))
                .withOutputTags(MAIN_DDL_TAG, TupleTagList.of(SHADOW_TABLE_DDL_TAG)));
  }
}
