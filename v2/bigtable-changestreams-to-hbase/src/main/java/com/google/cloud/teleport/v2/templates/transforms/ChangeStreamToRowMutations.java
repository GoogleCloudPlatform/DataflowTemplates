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
package com.google.cloud.teleport.v2.templates.transforms;

import static com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation.MutationType.GARBAGE_COLLECTION;
import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.teleport.v2.templates.utils.RowMutationsBuilder;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RowMutations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts Bigtable change stream {@link com.google.cloud.bigtable.data.v2.models.RowMutation} to
 * their approximating HBase {@link RowMutations}.
 */
public class ChangeStreamToRowMutations {
  private static final Logger LOG = LoggerFactory.getLogger(ChangeStreamToRowMutations.class);

  /** Creates change stream converter transformer. */
  public static ConvertChangeStream convertChangeStream(boolean filterGCMutationsInput) {
    return new ConvertChangeStream(filterGCMutationsInput);
  }

  /**
   * Change stream converter that converts change stream mutations into RowMutation objects. Input
   * is Bigtable change stream KV.of(rowkey, changeStreamMutation), Output is KV.of(rowkey, hbase
   * RowMutations object)
   */
  public static class ConvertChangeStream
      extends PTransform<
          PCollection<KV<ByteString, ChangeStreamMutation>>,
          PCollection<KV<byte[], RowMutations>>> {

    private boolean bidirectionalReplicationEnabled;
    private String cbtQualifier;
    private String hbaseQualifier;
    private boolean filterGCMutations;

    /**
     * Call converter with this function with the necessary params to enable bidirectional
     * replication logic.
     *
     * @param bidirectionalReplicationEnabledInput whether bidirectional replication logic is
     *     enabled
     * @param cbtQualifierInput cbt qualifier keyword to tag outgoing {@link RowMutations} with
     * @param hbaseQualifierInput hbase qualifier keyword to filter incoming {@link
     *     com.google.cloud.bigtable.data.v2.models.Mutation} with
     * @return ConvertChangeStream object to be used in change stream conversion transformations
     */
    public ConvertChangeStream withBidirectionalReplication(
        boolean bidirectionalReplicationEnabledInput,
        String cbtQualifierInput,
        String hbaseQualifierInput) {
      return new ConvertChangeStream(
          bidirectionalReplicationEnabledInput,
          cbtQualifierInput,
          hbaseQualifierInput,
          filterGCMutations);
    }

    public ConvertChangeStream(boolean filterGCMutationsInput) {
      filterGCMutations = filterGCMutationsInput;
    }

    private ConvertChangeStream(
        boolean bidirectionalReplicationEnabledInput,
        String cbtQualifierInput,
        String hbaseQualifierInput,
        boolean filterGCMutationsInput) {
      if (bidirectionalReplicationEnabledInput) {
        checkArgument(cbtQualifierInput != null, "cbt qualifier cannot be null.");
        checkArgument(hbaseQualifierInput != null, "hbase qualifier cannot be null.");
      }
      bidirectionalReplicationEnabled = bidirectionalReplicationEnabledInput;
      cbtQualifier = cbtQualifierInput;
      hbaseQualifier = hbaseQualifierInput;
      filterGCMutations = filterGCMutationsInput;
    }

    @Override
    public PCollection<KV<byte[], RowMutations>> expand(
        PCollection<KV<ByteString, ChangeStreamMutation>> input) {
      return input.apply(
          ParDo.of(
              new ConvertChangeStreamFn(
                  bidirectionalReplicationEnabled,
                  cbtQualifier,
                  hbaseQualifier,
                  filterGCMutations)));
    }
  }

  /** Converts Bigtable change stream mutations to Hbase RowMutations objects. */
  public static class ConvertChangeStreamFn
      extends DoFn<KV<ByteString, ChangeStreamMutation>, KV<byte[], RowMutations>> {

    private static final Logger LOG = LoggerFactory.getLogger(ConvertChangeStreamFn.class);

    private String hbaseQualifier;
    private String cbtQualifier;
    private boolean bidirectionalReplicationEnabled;

    private boolean filterGCMutations;

    public ConvertChangeStreamFn(
        boolean bidirectionalReplicationEnabledInput,
        String cbtQualifierInput,
        String hbaseQualifierInput,
        boolean filterGCMutationsInput) {
      bidirectionalReplicationEnabled = bidirectionalReplicationEnabledInput;
      hbaseQualifier = hbaseQualifierInput;
      cbtQualifier = cbtQualifierInput;
      filterGCMutations = filterGCMutationsInput;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      ChangeStreamMutation mutation = c.element().getValue();

      // Skip element if filter GC flag on and the mutation was of GC type.
      if (filterGCMutations && mutation.getType().equals(GARBAGE_COLLECTION)) {
        Metrics.counter(ConvertChangeStreamFn.class, "gc_mutations_filtered").inc();
        return;
      }

      // Skip element if it was replicated from HBase.
      if (bidirectionalReplicationEnabled && isHbaseReplicated(mutation, hbaseQualifier)) {
        return;
      }
      RowMutations hbaseMutations = RowMutationsBuilder.buildRowMutations(mutation);
      // Append origin information to mutations.
      if (bidirectionalReplicationEnabled) {
        appendSourceTagToMutations(hbaseMutations, cbtQualifier);
      }
      c.output(KV.of(hbaseMutations.getRow(), hbaseMutations));
    }

    /**
     * Checks if mutation was replicated from HBase.
     *
     * @param mutation from change stream
     * @return true if mutation was replicated from hbase
     */
    private boolean isHbaseReplicated(ChangeStreamMutation mutation, String hbaseQualifierInput) {
      List<Entry> mutationEntries = mutation.getEntries();

      if (mutationEntries.size() == 0) {
        return false;
      }
      Entry lastEntry = mutationEntries.get(mutationEntries.size() - 1);

      if (lastEntry instanceof DeleteCells) {
        if (((DeleteCells) lastEntry)
            .getQualifier()
            .equals(ByteString.copyFromUtf8(hbaseQualifierInput))) {
          Metrics.counter(ConvertChangeStreamFn.class, "hbase_mutations_filtered").inc();
          return true;
        }
      }
      Metrics.counter(ConvertChangeStreamFn.class, "bigtable_mutations_replicated").inc();
      return false;
    }

    static byte[] convertUtf8String(String string) {
      return string.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Appends origin information to row mutation for bidirectional replication. The Hbase-Bigtable
     * replicator at the destination Hbase will check for this source tag and filter out the
     * mutation that this replicator sends out. This prevents replication loops from forming.
     *
     * @param hbaseMutations row mutation to append origin info to
     * @param cbtQualifierInput origin info string denoting mutation is from bigtable
     * @throws IOException
     */
    private void appendSourceTagToMutations(RowMutations hbaseMutations, String cbtQualifierInput)
        throws IOException {
      byte[] lastEntryCf = getLastCellColFamily(hbaseMutations);
      Delete hiddenDelete = new Delete(hbaseMutations.getRow(), 0L);
      hiddenDelete.addColumns(lastEntryCf, convertUtf8String(cbtQualifierInput));
      hbaseMutations.add(hiddenDelete);
    }

    /**
     * Gets column family from last mutation of row mutations.
     *
     * @param hbaseMutations rowMutations object
     * @return columnFamily byte array
     * @throws IOException
     */
    private byte[] getLastCellColFamily(RowMutations hbaseMutations) throws IOException {
      Mutation lastMutation =
          hbaseMutations.getMutations().get(hbaseMutations.getMutations().size() - 1);
      CellScanner scanner = lastMutation.cellScanner();
      // Get last cell of last mutation.
      Cell cell = scanner.current();
      while (scanner.advance()) {
        cell = scanner.current();
      }
      return CellUtil.cloneFamily(cell);
    }
  }
}
