/*
 * Copyright (C) 2018 Google LLC
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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.spanner.ddl.Table;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Base64;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A transform compares databases and returns a PCollection with the number of mismatched records.
 */
public class CompareDatabases extends PTransform<PBegin, PCollection<Long>> {

  private final SpannerConfig one;
  private final SpannerConfig two;

  public CompareDatabases(SpannerConfig one, SpannerConfig two) {
    this.one = one;
    this.two = two;
  }

  @Override
  public PCollection<Long> expand(PBegin begin) {

    final TupleTag<Struct> oneTag = new TupleTag<>();
    PCollection<KV<String, Struct>> rowsOne = begin.apply("Read one", new ReadAllRows(one));
    final TupleTag<Struct> twoTag = new TupleTag<>();
    PCollection<KV<String, Struct>> rowsTwo = begin.apply("Read two", new ReadAllRows(two));

    PCollection<KV<String, CoGbkResult>> cogroup =
        KeyedPCollectionTuple.of(oneTag, rowsOne).and(twoTag, rowsTwo).apply(CoGroupByKey.create());

    PCollection<String> fails =
        cogroup.apply(
            ParDo.of(
                new DoFn<KV<String, CoGbkResult>, String>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    KV<String, CoGbkResult> element = c.element();
                    CoGbkResult gbk = element.getValue();
                    ArrayList<Struct> oneRows = Lists.newArrayList(gbk.getAll(oneTag));
                    ArrayList<Struct> twoRows = Lists.newArrayList(gbk.getAll(twoTag));

                    if (oneRows.size() != 1 || twoRows.size() != 1) {
                      c.output(element.getKey());
                      return;
                    }

                    Struct sOne = oneRows.get(0);
                    Struct sTwo = twoRows.get(0);

                    if (!sOne.equals(sTwo)) {
                      c.output(element.getKey());
                    }
                  }
                }));

    return fails.apply(Count.globally());
  }

  private static class ReadAllRows extends PTransform<PBegin, PCollection<KV<String, Struct>>> {

    private final SpannerConfig spanConfig;

    private ReadAllRows(SpannerConfig spanConfig) {
      this.spanConfig = spanConfig;
    }

    @Override
    public PCollection<KV<String, Struct>> expand(PBegin begin) {
      PCollectionView<Transaction> tx =
          begin.apply(SpannerIO.createTransaction().withSpannerConfig(spanConfig));

      PCollectionView<Dialect> dialectView =
          begin
              .apply("Read Dialect", new ReadDialect(spanConfig))
              .apply("As PCollectionView", View.asSingleton());

      PCollection<Ddl> sourceDdl =
          begin.apply(
              "Read Information Schema", new ReadInformationSchema(spanConfig, tx, dialectView));

      final PCollectionView<Ddl> ddlView = sourceDdl.apply(View.asSingleton());

      PCollection<ReadOperation> tables =
          sourceDdl.apply(
              new BuildReadFromTableOperations(ValueProvider.StaticValueProvider.of("")));

      PCollection<Struct> rows =
          tables.apply(
              "Read rows from tables",
              SpannerIO.readAll().withTransaction(tx).withSpannerConfig(spanConfig));

      return rows.apply(
          ParDo.of(
                  new DoFn<Struct, KV<String, Struct>>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      Ddl ddl = c.sideInput(ddlView);
                      Struct struct = c.element();
                      String tableName = struct.getString(0);

                      Table table = ddl.table(tableName);
                      String key = tableName;
                      for (IndexColumn pk : table.primaryKeys()) {
                        Type columnType = struct.getColumnType(pk.name());
                        if (struct.isNull(pk.name())) {
                          key += "-NULL-";
                          continue;
                        }
                        switch (columnType.getCode()) {
                          case BOOL:
                            key += struct.getBoolean(pk.name());
                            break;
                          case INT64:
                            key += struct.getLong(pk.name());
                            break;
                          case STRING:
                          case PG_NUMERIC:
                            key += struct.getString(pk.name());
                            break;
                          case BYTES:
                            key +=
                                Base64.getEncoder()
                                    .encodeToString(struct.getBytes(pk.name()).toByteArray());
                            break;
                          case FLOAT64:
                            key += struct.getDouble(pk.name());
                            break;
                          case TIMESTAMP:
                            key += struct.getTimestamp(pk.name());
                            break;
                          case DATE:
                            key += struct.getDate(pk.name());
                            break;
                          case NUMERIC:
                            key += struct.getBigDecimal(pk.name());
                            break;
                          default:
                            throw new IllegalArgumentException("Unsupported PK type " + columnType);
                        }
                      }
                      c.output(KV.of(key, struct));
                    }
                  })
              .withSideInputs(ddlView));
    }
  }
}
