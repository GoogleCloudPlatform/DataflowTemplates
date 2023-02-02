/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.utils;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility functions to help assert equality between mutation lists for testing purposes.
 *
 * <p>RowMutations objects assert equality on rowkey only and does not guarantee that its mutations
 * are the same nor that they are in the same order. This class hashes a RowMutation object's
 * mutations so that two mutation lists can be compared for equality.
 */
public class HashUtils {

  /**
   * Hashes list of mutations into String, so lisst of mutations can be compared to other mutations.
   *
   * @param mutationList
   * @return list of mutation strings that can be compared to other hashed mutation lists.
   */
  public static List<String> hashMutationList(List<Mutation> mutationList) throws Exception {
    List<String> mutations = new ArrayList<>();
    for (Mutation mutation : mutationList) {
      List<String> cells = new ArrayList<>();

      CellScanner scanner = mutation.cellScanner();
      while (scanner.advance()) {
        Cell c = scanner.current();
        String mutationType = "";
        long ts = 0;

        if (c.getType() == Cell.Type.DeleteFamily) {
          // DeleteFamily has its timestamp created at runtime and cannot be compared with accuracy
          // during tests, so we remove the timestamp altogether.
          mutationType = "DELETE_FAMILY";
          ts = 0L;
        } else if (c.getType() == Cell.Type.DeleteColumn) {
          mutationType = "DELETE_COLUMN";
          ts = c.getTimestamp();
        } else if (c.getType() == Cell.Type.Put) {
          mutationType = "PUT";
          ts = c.getTimestamp();
        } else {
          throw new Exception("hashMutationList error: Cell type is not supported.");
        }

        String cellHash =
            String.join(
                "_",
                mutationType,
                Long.toString(ts),
                Bytes.toString(CellUtil.cloneRow(c)),
                Bytes.toString(CellUtil.cloneFamily(c)),
                Bytes.toString(CellUtil.cloneQualifier(c)),
                Bytes.toString(CellUtil.cloneValue(c)));

        cells.add(cellHash);
      }

      mutations.add(String.join("; ", cells));
    }

    return mutations;
  }

  /** Utility transformation to split RowMutations object into <rowkey, mutationsHashList>. */
  public static class HashHbaseRowMutations
      extends PTransform<
          PCollection<KV<byte[], RowMutations>>, PCollection<KV<String, List<String>>>> {

    private final Logger log = LoggerFactory.getLogger(HashHbaseRowMutations.class);

    @Override
    public PCollection<KV<String, List<String>>> expand(
        PCollection<KV<byte[], RowMutations>> input) {
      return input.apply(ParDo.of(new HashHbaseRowMutationsFn()));
    }
  }

  /** Utility function that applies mutation list hash to elements. */
  static class HashHbaseRowMutationsFn
      extends DoFn<KV<byte[], RowMutations>, KV<String, List<String>>> {
    private final Logger log = LoggerFactory.getLogger(HashHbaseRowMutationsFn.class);

    public HashHbaseRowMutationsFn() {}

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      RowMutations rowMutations = c.element().getValue();

      if (!new String(c.element().getKey()).equals(new String(rowMutations.getRow()))) {
        throw new Exception(
            "error during hash row check:"
                + new String(c.element().getKey())
                + "!="
                + new String(rowMutations.getRow())
                + ".");
      }

      c.output(
          KV.of(
              new String(rowMutations.getRow()),
              hashMutationList(rowMutations.getMutations()) // mutations.getMutations())
              ));
    }
  }
}
