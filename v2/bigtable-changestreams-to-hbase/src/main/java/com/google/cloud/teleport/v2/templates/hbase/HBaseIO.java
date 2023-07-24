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
package com.google.cloud.teleport.v2.templates.hbase;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class HBaseIO {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseIO.class);

  /** Disallow construction of utility class. */
  private HBaseIO() {}

  public static WriteRowMutations writeRowMutations() {
    return new WriteRowMutations(null /* Configuration */, "");
  }

  /** Transformation that writes RowMutation objects to a Hbase table. */
  public static class WriteRowMutations
      extends PTransform<PCollection<KV<byte[], RowMutations>>, PDone> {

    /** Writes to the HBase instance indicated by the given Configuration. */
    public WriteRowMutations withConfiguration(Configuration configuration) {
      checkNotNull(configuration, "configuration cannot be null");
      return new WriteRowMutations(configuration, tableId);
    }

    /** Writes to the specified table. */
    public WriteRowMutations withTableId(String tableId) {
      checkNotNull(tableId, "tableId cannot be null");
      return new WriteRowMutations(configuration, tableId);
    }

    private WriteRowMutations(Configuration configuration, String tableId) {
      this.configuration = configuration;
      this.tableId = tableId;
    }

    @Override
    public PDone expand(PCollection<KV<byte[], RowMutations>> input) {
      checkNotNull(configuration, "withConfiguration() is required");
      checkNotNull(tableId, "withTableId() is required");
      checkArgument(!tableId.isEmpty(), "withTableId() cannot be empty");

      input.apply(ParDo.of(new WriteRowMutationsFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("configuration", configuration.toString()));
      builder.add(DisplayData.item("tableId", tableId));
    }

    public Configuration getConfiguration() {
      return configuration;
    }

    public String getTableId() {
      return tableId;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      WriteRowMutations writeRowMutations = (WriteRowMutations) o;
      return configuration.toString().equals(writeRowMutations.configuration.toString())
          && Objects.equals(tableId, writeRowMutations.tableId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(configuration, tableId);
    }

    /**
     * The writeReplace method allows the developer to provide a replacement object that will be
     * serialized instead of the original one. We use this to keep the enclosed class immutable. For
     * more details on the technique see <a
     * href="https://lingpipe-blog.com/2009/08/10/serializing-immutable-singletons-serialization-proxy/">this
     * article</a>.
     */
    private Object writeReplace() {
      return new SerializationProxy(this);
    }

    private static class SerializationProxy implements Serializable {
      public SerializationProxy() {}

      public SerializationProxy(WriteRowMutations writeRowMutations) {
        configuration = writeRowMutations.configuration;
        tableId = writeRowMutations.tableId;
      }

      private void writeObject(ObjectOutputStream out) throws IOException {
        SerializableCoder.of(SerializableConfiguration.class)
            .encode(new SerializableConfiguration(this.configuration), out);

        StringUtf8Coder.of().encode(this.tableId, out);
      }

      private void readObject(ObjectInputStream in) throws IOException {
        this.configuration = SerializableCoder.of(SerializableConfiguration.class).decode(in).get();
        this.tableId = StringUtf8Coder.of().decode(in);
      }

      Object readResolve() {
        return HBaseIO.writeRowMutations().withConfiguration(configuration).withTableId(tableId);
      }

      private Configuration configuration;
      private String tableId;
    }

    @SuppressFBWarnings("SE_BAD_FIELD")
    private final Configuration configuration;

    private final String tableId;

    /** Function to write row mutations to a hbase table. */
    private class WriteRowMutationsFn extends DoFn<KV<byte[], RowMutations>, Integer> {

      public WriteRowMutationsFn(WriteRowMutations writeRowMutations) {
        checkNotNull(writeRowMutations.tableId, "tableId");
        checkNotNull(writeRowMutations.configuration, "configuration");
      }

      @Setup
      public void setup() throws Exception {
        connection = HBaseSharedConnection.getOrCreate(configuration);
      }

      @StartBundle
      public void startBundle(StartBundleContext c) throws IOException {
        table = connection.getTable(TableName.valueOf(tableId));
        recordsWritten = 0;
      }

      @FinishBundle
      public void finishBundle() throws Exception {
        if (table != null) {
          table.close();
          table = null;
        }

        LOG.debug("Wrote {} records", recordsWritten);
      }

      @Teardown
      public void tearDown() throws Exception {

        if (table != null) {
          table.close();
          table = null;
        }

        HBaseSharedConnection.close(configuration);
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws IOException {
        RowMutations mutations = c.element().getValue();

        try {
          // Use Table instead of BufferedMutator to preserve mutation-ordering
          table.mutateRow(mutations);
          recordsWritten++;
        } catch (IOException e) {
          throw new RuntimeException(
              (String.join(
                  " ",
                  "Table",
                  tableId,
                  "row",
                  Bytes.toString(mutations.getRow()),
                  "mutation failed.",
                  "\nTable Available/Enabled:",
                  Boolean.toString(
                      connection.getAdmin().isTableAvailable(TableName.valueOf(tableId))),
                  Boolean.toString(
                      connection.getAdmin().isTableEnabled(TableName.valueOf(tableId))),
                  "\nConnection Closed/Aborted/Locks:",
                  Boolean.toString(connection.isClosed()),
                  Boolean.toString(connection.isAborted()))));
        }
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.delegate(WriteRowMutations.this);
      }

      private long recordsWritten;
      private transient Connection connection;
      private transient Table table;
    }
  }
}
