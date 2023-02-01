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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
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
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converts Bigtable change stream RowMutations objects to their approximating HBase mutations. */
public class ConvertChangeStream {
  private static final Logger LOG = LoggerFactory.getLogger(ConvertChangeStream.class);

  /** Creates change stream converter transformer. */
  public static ConvertChangeStreamMutation convertChangeStreamMutation() {
    return new ConvertChangeStreamMutation();
  }

  /**
   * Change stream converter that converts change stream mutations into RowMutation objects. Input
   * is Bigtable change stream KV.of(rowkey, changeStreamMutation), Output is KV.of(rowkey, hbase
   * RowMutations object)
   */
  public static class ConvertChangeStreamMutation
      extends PTransform<
          PCollection<KV<ByteString, ChangeStreamMutation>>,
          PCollection<KV<byte[], RowMutations>>> {

    /**
     * Call converter with this function with the necessary params to enable two-way replication
     * logic.
     *
     * @param enabled whether two-way replication logic is enabled
     * @param cbtQualifierInput
     * @param hbaseQualifierInput
     */
    public ConvertChangeStreamMutation withTwoWayReplication(
        boolean enabled, String cbtQualifierInput, String hbaseQualifierInput) {
      return new ConvertChangeStreamMutation(enabled, cbtQualifierInput, hbaseQualifierInput);
    }

    public ConvertChangeStreamMutation() {}

    private ConvertChangeStreamMutation(
        boolean enabled, String cbtQualifierInput, String hbaseQualifierInput) {
      if (enabled) {
        checkArgument(cbtQualifierInput != null, "cbt qualifier cannot be null.");
        checkArgument(hbaseQualifierInput != null, "hbase qualifier cannot be null.");
      }
      twoWayReplication = enabled;
      cbtQualifier = cbtQualifierInput;
      hbaseQualifier = hbaseQualifierInput;
    }

    private boolean twoWayReplication;
    private String cbtQualifier;
    private String hbaseQualifier;

    @Override
    public PCollection<KV<byte[], RowMutations>> expand(
        PCollection<KV<ByteString, ChangeStreamMutation>> input) {
      return input.apply(
          ParDo.of(
              new ConvertChangeStreamMutationFn(twoWayReplication, cbtQualifier, hbaseQualifier)));
    }
  }

  /** Converts Bigtable change stream mutations to Hbase RowMutations objects. */
  public static class ConvertChangeStreamMutationFn
      extends DoFn<KV<ByteString, ChangeStreamMutation>, KV<byte[], RowMutations>> {

    private static final Logger LOG = LoggerFactory.getLogger(ConvertChangeStreamMutationFn.class);

    private String hbaseQualifier;
    private String cbtQualifier;
    private boolean twoWayReplication;

    public ConvertChangeStreamMutationFn(
        boolean twoWayReplicationFlag, String cbtQualifierInput, String hbaseQualifierInput) {
      twoWayReplication = twoWayReplicationFlag;
      hbaseQualifier = hbaseQualifierInput;
      cbtQualifier = cbtQualifierInput;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {

      ChangeStreamMutation mutation = c.element().getValue();

      // Skip element if it was replicated from HBase.
      if (twoWayReplication && isHbaseReplicated(mutation, hbaseQualifier)) {
        return;
      }
      RowMutations hbaseMutations = convertToRowMutations(mutation);

      // Append origin information to mutations.
      if (twoWayReplication) {
        appendOriginInfoToMutations(hbaseMutations, cbtQualifier);
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
          LOG.info(
              "Filtering: {}",
              mutation.toRowMutation("").toProto(RequestContext.create("", "", "")));
          Metrics.counter("HbaseRepl", "mutations_filtered_from_hbase").inc();
          return true;
        }
      }
      Metrics.counter("HbaseRepl", "mutations_valid_from_bigtable").inc();
      return false;
    }

    static byte[] convertUtf8String(String utf8) {
      return utf8.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Appends origin information to row mutation.
     *
     * @param hbaseMutations row mutation to append origin info to
     * @param cbtQualifierInput origin info string denoting mutation is from bigtable
     * @throws IOException
     */
    private void appendOriginInfoToMutations(RowMutations hbaseMutations, String cbtQualifierInput)
        throws IOException {
      // Get last mutation to deduce last column family from
      Mutation lastMutation =
          hbaseMutations.getMutations().get(hbaseMutations.getMutations().size() - 1);
      List<String> familyList = (List<String>) lastMutation.getFingerprint().get("families");
      String lastEntryCf = familyList.get(familyList.size() - 1);
      // If this is null it means we didn't process any mutation entries. this
      // shouldn't be possible once all mutation types are supported
      if (lastEntryCf != null) {
        Delete hiddenDelete = new Delete(hbaseMutations.getRow(), 0L);
        hiddenDelete.addColumn(
            convertUtf8String(lastEntryCf), convertUtf8String(cbtQualifierInput));
        hbaseMutations.add(hiddenDelete);
      }
    }

    /**
     * Converts Cdc ChangeStreamMutation to HBase RowMutations object.
     *
     * @param mutation changeStreamMutation
     * @return Hbase RowMutations object
     */
    private static RowMutations convertToRowMutations(ChangeStreamMutation mutation)
        throws IOException {
      byte[] hbaseRowKey = mutation.getRowKey().toByteArray();
      RowMutations hbaseMutations = new RowMutations(hbaseRowKey);

      for (Entry entry : mutation.getEntries()) {
        if (entry instanceof SetCell) {

          SetCell setCell = (SetCell) entry;
          Put put = new Put(hbaseRowKey, setCell.getTimestamp() / 1000);
          put.addColumn(
              convertUtf8String(setCell.getFamilyName()),
              setCell.getQualifier().toByteArray(),
              setCell.getValue().toByteArray());
          hbaseMutations.add(put);

        } else if (entry instanceof DeleteCells) {

          DeleteCells deleteCells = (DeleteCells) entry;
          Delete delete = new Delete(hbaseRowKey, deleteCells.getTimestampRange().getEnd() / 1000);
          delete.addColumn(
              convertUtf8String(deleteCells.getFamilyName()),
              deleteCells.getQualifier().toByteArray());
          hbaseMutations.add(delete);

        } else if (entry instanceof DeleteFamily) {

          // Bigtable deletefamily does not assume a temporal dimension because it deletes
          // everything
          // that came before it from the POV of row-level transaction sequence.
          // Hbase deletes operate from timestamps only. Therefore, we approximate Bigtable
          // deletefamily with Hbase deletefamily with timestamp now().
          long now = Time.now();

          DeleteFamily deleteCells = (DeleteFamily) entry;
          Delete delete = new Delete(hbaseRowKey, now);
          delete.addFamily(convertUtf8String(deleteCells.getFamilyName()));
          hbaseMutations.add(delete);
        }
      }
      return hbaseMutations;
    }
  }
}
