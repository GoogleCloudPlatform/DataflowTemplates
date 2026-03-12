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
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.mysql.MySqlSchemaFetcher;
import com.google.cloud.teleport.v2.templates.sink.SinkSchemaFetcher;
import com.google.cloud.teleport.v2.templates.spanner.SpannerSchemaFetcher;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that loads the {@link DataGeneratorSchema} from the sink as a side input.
 */
public class SchemaLoader extends PTransform<PBegin, PCollectionView<DataGeneratorSchema>> {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaLoader.class);
  protected final SinkType sinkType;
  protected final String sinkOptionsPath;
  protected final Integer insertQps;

  private static final Map<SinkType, Supplier<SinkSchemaFetcher>> fetcherRegistry =
      new EnumMap<>(SinkType.class);

  static {
    fetcherRegistry.put(SinkType.SPANNER, SpannerSchemaFetcher::new);
    fetcherRegistry.put(SinkType.MYSQL, MySqlSchemaFetcher::new);
    // Register new sink fetcher implementations here
  }

  public SchemaLoader(SinkType sinkType, String sinkOptionsPath, Integer insertQps) {
    this.sinkType = sinkType;
    this.sinkOptionsPath = sinkOptionsPath;
    this.insertQps = insertQps;
  }

  @Override
  public PCollectionView<DataGeneratorSchema> expand(PBegin input) {
    return input
        .apply("CreateSinkType", Create.of(sinkType))
        .apply("FetchSchema", ParDo.of(new FetchSchemaFn(sinkType, sinkOptionsPath, insertQps)))
        .apply("ViewAsSingleton", View.asSingleton());
  }

  class FetchSchemaFn extends DoFn<SinkType, DataGeneratorSchema> {
    private final SinkType sinkType;
    private final String sinkOptionsPath;
    private final int insertQps;

    FetchSchemaFn(SinkType sinkType, String sinkOptionsPath, Integer insertQps) {
      this.sinkType = sinkType;
      this.sinkOptionsPath = sinkOptionsPath;
      this.insertQps = insertQps != null ? insertQps : 1;
    }

    @ProcessElement
    public void processElement(OutputReceiver<DataGeneratorSchema> receiver) {
      try {
        String sinkOptionsJson = readSinkOptions(sinkOptionsPath);
        SinkSchemaFetcher fetcher = createFetcher(sinkType);

        fetcher.init(sinkOptionsPath, sinkOptionsJson);
        fetcher.setInsertQps(insertQps);
        DataGeneratorSchema schema = fetcher.getSchema();
        LOG.info("Fetched Schema: {}", schema);
        receiver.output(schema);
      } catch (IOException e) {
        throw new RuntimeException("Failed to fetch schema", e);
      }
    }

    /**
     * Creates a {@link SinkSchemaFetcher} based on the sink type using the registry.
     *
     * @param sinkType The sink type.
     * @return The {@link SinkSchemaFetcher}.
     */
    protected SinkSchemaFetcher createFetcher(SinkType sinkType) {
      Supplier<SinkSchemaFetcher> fetcherSupplier = fetcherRegistry.get(sinkType);
      if (fetcherSupplier != null) {
        return fetcherSupplier.get();
      } else {
        throw new IllegalArgumentException("Unsupported sink type: " + sinkType);
      }
    }

    protected String readSinkOptions(String path) throws IOException {
      try (ReadableByteChannel channel =
          FileSystems.open(FileSystems.matchNewResource(path, false))) {
        try (Reader reader =
            new InputStreamReader(Channels.newInputStream(channel), StandardCharsets.UTF_8)) {
          return CharStreams.toString(reader);
        }
      }
    }
  }
}
