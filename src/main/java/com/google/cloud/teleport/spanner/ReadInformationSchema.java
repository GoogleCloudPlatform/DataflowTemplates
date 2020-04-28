/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.spanner;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.InformationSchemaScanner;
import org.apache.beam.sdk.io.gcp.spanner.ExposedSpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/** {@link InformationSchemaScanner} as a Beam transform. */
public class ReadInformationSchema extends PTransform<PBegin, PCollection<Ddl>> {

  private final SpannerConfig spannerConfig;
  private final PCollectionView<Transaction> tx;

  public ReadInformationSchema(SpannerConfig spannerConfig, PCollectionView<Transaction> tx) {
    this.spannerConfig = spannerConfig;
    this.tx = tx;
  }

  @Override
  public PCollection<Ddl> expand(PBegin p) {
    return p.apply("Create empty", Create.of((Void) null))
        .apply(
            "Read Information Schema",
            ParDo.of(new ReadInformationSchemaFn(spannerConfig, tx)).withSideInputs(tx));
  }

  private static class ReadInformationSchemaFn extends DoFn<Void, Ddl> {
    private final SpannerConfig spannerConfig;
    private transient ExposedSpannerAccessor spannerAccessor;
    private final PCollectionView<Transaction> tx;

    public ReadInformationSchemaFn(SpannerConfig spannerConfig, PCollectionView<Transaction> tx) {
      this.spannerConfig = spannerConfig;
      this.tx = tx;
    }

    @Setup
    public void setup() throws Exception {
      spannerAccessor = ExposedSpannerAccessor.create(spannerConfig);
    }

    @Teardown
    public void teardown() throws Exception {
      spannerAccessor.close();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Transaction transaction = c.sideInput(tx);
      BatchTransactionId transactionId = transaction.transactionId();

      BatchClient batchClient = spannerAccessor.getBatchClient();

      BatchReadOnlyTransaction context = batchClient.batchReadOnlyTransaction(transactionId);

      InformationSchemaScanner scanner = new InformationSchemaScanner(context);
      Ddl ddl = scanner.scan();
      c.output(ddl);
    }
  }
}
