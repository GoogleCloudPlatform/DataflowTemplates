/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.spanner;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import org.apache.beam.sdk.io.gcp.spanner.LocalSpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/** Retrieve {@link Dialect} from Spanner database for further usage. */
public class ReadDialect extends PTransform<PBegin, PCollection<Dialect>> {

  private final SpannerConfig spannerConfig;

  public ReadDialect(SpannerConfig spannerConfig) {
    this.spannerConfig = spannerConfig;
  }

  @Override
  public PCollection<Dialect> expand(PBegin p) {
    return p.apply("Create empty", Create.of((Void) null))
        .apply("Read dialect from Spanner", ParDo.of(new ReadDialectFn(spannerConfig)));
  }

  private static class ReadDialectFn extends DoFn<Void, Dialect> {
    private final SpannerConfig spannerConfig;
    private transient LocalSpannerAccessor spannerAccessor;

    public ReadDialectFn(SpannerConfig spannerConfig) {
      this.spannerConfig = spannerConfig;
    }

    @Setup
    public void setup() throws Exception {
      spannerAccessor = LocalSpannerAccessor.getOrCreate(spannerConfig);
    }

    @Teardown
    public void teardown() throws Exception {
      spannerAccessor.close();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      DatabaseClient databaseClient = spannerAccessor.getDatabaseClient();
      Dialect dialect = databaseClient.getDialect();
      c.output(dialect);
    }
  }
}
