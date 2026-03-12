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
package com.google.cloud.teleport.v2.templates.transforms;

import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.dofn.FetchSchemaFn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.mysql.MySqlSchemaFetcher;
import com.google.cloud.teleport.v2.templates.sink.SinkSchemaFetcher;
import com.google.cloud.teleport.v2.templates.spanner.SpannerSchemaFetcher;
import com.google.common.annotations.VisibleForTesting;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * A {@link PTransform} that loads the {@link DataGeneratorSchema} from the sink as a side input.
 */
public class SchemaLoader extends PTransform<PBegin, PCollectionView<DataGeneratorSchema>> {

  protected final SinkType sinkType;
  protected final String sinkOptionsPath;
  protected final Integer insertQps;
  private final FetchSchemaFn customFn;

  private static final Map<SinkType, Supplier<SinkSchemaFetcher>> fetcherRegistry =
      new EnumMap<>(SinkType.class);

  static {
    fetcherRegistry.put(SinkType.SPANNER, SpannerSchemaFetcher::new);
    fetcherRegistry.put(SinkType.MYSQL, MySqlSchemaFetcher::new);
    // Register new sink fetcher implementations here
  }

  public SchemaLoader(SinkType sinkType, String path, Integer qps) {
    this(sinkType, path, qps, null);
  }

  // Internal constructor for testing
  @VisibleForTesting
  SchemaLoader(SinkType sinkType, String path, Integer qps, FetchSchemaFn customFn) {
    this.sinkType = sinkType;
    this.sinkOptionsPath = path;
    this.insertQps = qps;
    this.customFn = customFn;
  }

  @Override
  public PCollectionView<DataGeneratorSchema> expand(PBegin input) {

    FetchSchemaFn fetchFn =
        (customFn != null) ? customFn : new FetchSchemaFn(sinkType, sinkOptionsPath, insertQps);
    return input
        .apply("CreateSinkType", Create.of(sinkType))
        .apply("FetchSchema", ParDo.of(fetchFn))
        .apply("ViewAsSingleton", View.asSingleton());
  }
}
