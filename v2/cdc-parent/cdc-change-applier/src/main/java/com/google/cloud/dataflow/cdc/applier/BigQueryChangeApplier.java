/*
 * Copyright (C) 2019 Google Inc.
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
package com.google.cloud.dataflow.cdc.applier;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.cdc.common.DataflowCdcRowFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform} that consumes a {@link PCollection} of change data, and syncs with BigQuery.
 * This transform relies on keeping two separate BigQuery tables: One containing a log of changes,
 * and another being a replica of the source table.
 *
 * The input change data are represented as Beam-encoded {@link Row}s containing these fields:
 * <ul>
 *   <li>{@code operation} of type {@link String}, containing the kind of operation
 *       performed (e.g. INSERT, UPDATE, DELETE).</li>
 *   <li>{@code tableName} of type {@link String}, containing the full table name from the
 *       data source (i.e. {@code ${INSTANCE}.${DATABASE}.${TABLE_NAME}})</li>
 *   <li>{@code primaryKey} of type {@link Row}, containing the row's primary key.</li>
 *   <li>{@code fullRecord} of type {@link Row}, containing the full row after the change.</li>
 *   <li>{@code timestampMs} of type {@link Long}, containing the timestamp of the change.</li>
 * </ul>
 *
 * This PTransform has two main paths: One fast path that contains the change data, where multiple
 * elements may be processed every second; and a <i>slow</i> path, where BigQuery Merge statements
 * are periodically issued to update the replica table with the new record updates available in the
 * changelog table.
 */
public class BigQueryChangeApplier extends PTransform<PCollection<Row>, PDone> {

  private final String changeLogDataset;
  private final String replicaDataset;
  private final Integer updateFrequencySeconds;

  private final String gcpProjectId;

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryChangeApplier.class);

  /**
   * Creates a {@link PTransform} that will apply changes from the input {@link PCollection} into
   * tables stored in the given {@code replicaDataset}.
   *
   * It uses the {@code changeLogDataset} to keep tables with the logs of changes to be applied
   * to tables in the {@code replicaDataset}. The {@code changeLogDataset}, and the
   * {@code replicaDataset} may be the same. Both datasets must reside in the same GCP project,
   * dictated by {@code gcpProjectId}.
   *
   * The changelog is synchronized to the replica with a frequency of at most
   * {@code updateFrequencySeconds}.
   */
  public static BigQueryChangeApplier of(
      String changeLogDataset,
      String replicaDataset,
      Integer updateFrequencySeconds,
      String gcpProjectId) {
    return new BigQueryChangeApplier(
        changeLogDataset, replicaDataset, updateFrequencySeconds, gcpProjectId);
  }

  private BigQueryChangeApplier(
      String changeLogDataset,
      String replicaDataset,
      Integer updateFrequencySeconds,
      String gcpProjectId) {
    this.changeLogDataset = changeLogDataset;
    this.replicaDataset = replicaDataset;
    this.updateFrequencySeconds = updateFrequencySeconds;
    this.gcpProjectId = gcpProjectId;
  }

  @Override
  public PDone expand(PCollection<Row> input) {
    Pipeline p = input.getPipeline();
    Schema inputCollectionSchema = input.getSchema();

    PCollection<KV<String, KV<Schema, Schema>>> tableSchemaCollection =
        buildTableSchemaCollection(input);
    PCollectionView<Map<String, KV<Schema, Schema>>> schemaMapView = tableSchemaCollection
        .apply(View.asMap());

    PCollection<TableRow> updatesToWrite = formatIntoTableRows(input);

    updatesToWrite.apply(
        BigQueryIO.writeTableRows()
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withMethod(Method.STREAMING_INSERTS)
        .to(new ChangelogTableDynamicDestinations(changeLogDataset, gcpProjectId, schemaMapView)));

    String jobPrefix =
        String.format(
            "beam_cdc_%s_%s_", gcpProjectId.replace(':', '_').replace('.', '_'), replicaDataset);

    // If the input collection does not have a primary key field, then we do not need to issue
    // periodic merge requests.
    if (inputCollectionSchema.hasField(DataflowCdcRowFormat.PRIMARY_KEY)) {
      final Instant now = Instant.now();
      PCollection<KV<String, Long>> minTimestampInput = input
          .apply("KeyByTable", ParDo.of(new DoFn<Row, KV<String, Long>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              c.output(KV.of(
                      c.element().getString(DataflowCdcRowFormat.TABLE_NAME),
                      c.element().getInt64(DataflowCdcRowFormat.TIMESTAMP_MS)));
            }
          }))
          .apply(
              Window.<KV<String, Long>>into(new GlobalWindows())
                  .discardingFiredPanes()
                  .triggering(
                      Repeatedly.forever(
                          AfterProcessingTime.pastFirstElementInPane()
                              .plusDelayOf(Duration.ZERO)
                              .alignedTo(
                                  Duration.standardSeconds(updateFrequencySeconds),
                                      now))))
          .apply(Min.longsPerKey());

      PCollection<KV<String, KV<Schema, Schema>>> heartBeatInput = input
          .apply("KeyByTable", ParDo.of(new KeySchemasByTableFn(schemaMapView))
              .withSideInputs(schemaMapView))
          .apply(
              Window.<KV<String, KV<Schema, Schema>>>into(new GlobalWindows())
                  .discardingFiredPanes()
                  .triggering(
                      Repeatedly.forever(
                          AfterProcessingTime.pastFirstElementInPane()
                              .plusDelayOf(Duration.ZERO)
                              .alignedTo(
                                  Duration.standardSeconds(updateFrequencySeconds),
                                      now))))
          .apply(GroupByKey.create())
          .apply(
              ParDo.of(
                  new DoFn<
                      KV<String, Iterable<KV<Schema, Schema>>>,
                      KV<String, KV<Schema, Schema>>>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                      LOG.debug(
                          "TS: {} | Element: {} | Pane: {}", c.timestamp(), c.element(), c.pane());
                      Iterator<KV<Schema, Schema>> it = c.element().getValue().iterator();
                      if (it.hasNext()) {
                        c.output(KV.of(c.element().getKey(), it.next()));
                      }
                    }
                  }));

      final TupleTag<KV<Schema, Schema>> t1 = new TupleTag<>();
      final TupleTag<Long> t2 = new TupleTag<>();
      PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
              KeyedPCollectionTuple.of(t1, heartBeatInput)
                      .and(t2, minTimestampInput)
                      .apply(CoGroupByKey.<String>create());
      PCollection<KV<String, KV<KV<Schema, Schema>, Long>>> finalResultCollection =
              coGbkResultCollection.apply(ParDo.of(
                      new DoFn<KV<String, CoGbkResult>, KV<String, KV<KV<Schema, Schema>, Long>>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          KV<String, CoGbkResult> e = c.element();
                          KV<Schema, Schema> pt1Vals = e.getValue().getOnly(t1);
                          Long pt2Val = e.getValue().getOnly(t2);

                          c.output(KV.of(e.getKey(), KV.of(pt1Vals, pt2Val)));
                        }
                      }));

        finalResultCollection
          .apply("BuildMergeStatements",
              ParDo.of(
                  new MergeStatementBuildingFn(changeLogDataset, replicaDataset, gcpProjectId)))
          .setCoder(SerializableCoder.of(
              TypeDescriptors.kvs(
                  TypeDescriptors.strings(),
                  TypeDescriptor.of(BigQueryAction.class))))
          .apply("IssueMergeStatements",
              ParDo.of(new BigQueryStatementIssuingFn(jobPrefix)));
    }
    return PDone.in(p);
  }

  static PCollection<TableRow> formatIntoTableRows(PCollection<Row> input) {
    return input.apply("FormatUpdatesToTableRow",
        MapElements.into(TypeDescriptor.of(TableRow.class))
        .via(beamRow -> BigQueryUtils.toTableRow(beamRow)));
  }

  // TODO(pabloem): What about column-type changes?
  static PCollection<KV<String, KV<Schema, Schema>>> buildTableSchemaCollection(
      PCollection<Row> input) {
    // First we generate the map of tables, primary keys, and schemas
    // tableSchemaPair is a PCollection of KV pairs, where the Key is a table name, and
    // the Value is a pair of schemas: The Primary key schema, and the full Table schema.
    PCollection<KV<String, KV<Schema, Schema>>> tableSchemaPair = input
        // First we should filter out all empty records. Empty records will come from
        // DELETE-typed operations, which have the fullRecord field empty. Since this
        // PCollection is used to pass the table schema to the MERGE statement, we filter
        // those out.
        .apply("FilterEmptyRecords",
            Filter.by(
                r -> r.getRow(DataflowCdcRowFormat.FULL_RECORD) != null
                    && r.getRow(DataflowCdcRowFormat.FULL_RECORD).getSchema() != null))
        .apply("ExtractSchemas", MapElements.into(
            TypeDescriptors.kvs(
                TypeDescriptors.strings(),
                TypeDescriptors.kvs(
                    TypeDescriptor.of(Schema.class), TypeDescriptor.of(Schema.class))))
            .via(r -> KV.of(r.getString(DataflowCdcRowFormat.TABLE_NAME),
                KV.of(
                    r.getRow(DataflowCdcRowFormat.PRIMARY_KEY).getSchema(),
                    r.getRow(DataflowCdcRowFormat.FULL_RECORD).getSchema()))));

    // The collections of tableSchemaPairs is made into a side input. This side input is used
    // by the side of the pipeline that is in charge of issuing CREATE TABLE and MERGE statements
    // to BigQuery. The schema is used to derive the schema of the replica table; and also used
    // to build MERGE statements to refresh the replica table.
    // Specifically, because we know the name of the tables, and the schema, we use them to
    // issue joins over the primary key, and to create the replica table.
    return tableSchemaPair
        .apply(
            "GenerateSchemaCache",
            ParDo.of(
                new DoFn<KV<String, KV<Schema, Schema>>, KV<String, KV<Schema, Schema>>>() {
                  private final Logger log = LoggerFactory.getLogger("GenerateSchemaCache");

                  @StateId("knownSchemas")
                  private final StateSpec<ValueState<Map<String, KV<Schema, Schema>>>>
                      knownSchemas = StateSpecs.value();

                  private HashMap<String, KV<Schema, Schema>> seenTables = null;

                  @ProcessElement
                  public void processElement(
                      @Element KV<String, KV<Schema, Schema>> elm,
                      @StateId("knownSchemas")
                          ValueState<Map<String, KV<Schema, Schema>>> knownSchemasState,
                      OutputReceiver<KV<String, KV<Schema, Schema>>> outputReceiver) {
                    // Variable seenTables represents a per-bundle cache of tables that have been
                    // seen.
                    // We use this to avoid issuing multiple requests to keyed state per bundle.
                    if (seenTables == null) {
                      seenTables = new HashMap<>();
                    }

                    if (seenTables.containsKey(elm.getKey())) {
                      return;
                    }

                    seenTables.put(elm.getKey(), elm.getValue());

                    Map<String, KV<Schema, Schema>> knownSchemas = knownSchemasState.read();
                    if (knownSchemas == null) {
                      knownSchemas = new HashMap<>();
                    }

                    // If the schema is new, we output the new schema for the downstream transforms.
                    // This schema is used to build a side input, which in turn is used to issue
                    // MERGE statements to BigQuery.
                    KV<Schema, Schema> currentSchemas = knownSchemas.get(elm.getKey());
                    if (currentSchemas == null || !elm.getValue().equals(currentSchemas)) {
                      knownSchemas.put(elm.getKey(), elm.getValue());
                      knownSchemasState.write(knownSchemas);
                      log.info("New known schema: {}", elm);
                      outputReceiver.output(elm);
                    }
                  }
                }))
        .apply(
            Window.<KV<String, KV<Schema, Schema>>>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .discardingFiredPanes());
  }
}
