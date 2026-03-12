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
package com.google.cloud.teleport.v2.templates.fn;

import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.mysql.MySqlSchemaFetcher;
import com.google.cloud.teleport.v2.templates.sink.SinkSchemaFetcher;
import com.google.cloud.teleport.v2.templates.spanner.SpannerSchemaFetcher;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DoFn to fetch the schema from the sink. */
public class FetchSchemaFn extends DoFn<SinkType, DataGeneratorSchema> {
  private static final Logger LOG = LoggerFactory.getLogger(FetchSchemaFn.class);

  private static final Map<SinkType, Supplier<SinkSchemaFetcher>> fetcherProviders =
      ImmutableMap.of(
          SinkType.SPANNER, SpannerSchemaFetcher::new,
          SinkType.MYSQL, MySqlSchemaFetcher::new);

  private final SinkType sinkType;
  private final String sinkOptionsPath;
  private final int insertQps;

  public FetchSchemaFn(SinkType sinkType, String sinkOptionsPath, Integer insertQps) {
    this.sinkType = sinkType;
    this.sinkOptionsPath = sinkOptionsPath;
    this.insertQps = insertQps != null ? insertQps : 1;
  }

  @ProcessElement
  public void processElement(OutputReceiver<DataGeneratorSchema> receiver) {
    try {
      SinkSchemaFetcher fetcher = createFetcher(sinkType);

      fetcher.init(sinkOptionsPath);
      fetcher.setInsertQps(insertQps);
      DataGeneratorSchema schema = fetcher.getSchema();
      LOG.info("Fetched Schema: {}", schema);
      receiver.output(schema);
    } catch (IOException e) {
      throw new RuntimeException("Failed to fetch schema", e);
    }
  }

  private SinkSchemaFetcher createFetcher(SinkType sinkType) {
    Supplier<SinkSchemaFetcher> provider = fetcherProviders.get(sinkType);
    if (provider == null) {
      throw new IllegalArgumentException("Unsupported sink type: " + sinkType);
    }
    return provider.get();
  }
}
