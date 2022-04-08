/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.values.DatastreamRow;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code StatefulRowCleaner} class statefully processes and filters data based on the supplied
 * primary keys and sort keys in Datastream TableRow data.
 */
public class StatefulRowCleaner extends PTransform<PCollection<TableRow>, PCollectionTuple> {

  private static final Logger LOG = LoggerFactory.getLogger(StatefulRowCleaner.class);
  private static final String WINDOW_DURATION = "1s";

  public TupleTag<TableRow> successTag = new TupleTag<TableRow>() {};
  public TupleTag<TableRow> failureTag = new TupleTag<TableRow>() {};

  private StatefulRowCleaner() {}

  public static StatefulRowCleaner of() {
    return new StatefulRowCleaner();
  }

  @Override
  public PCollectionTuple expand(PCollection<TableRow> input) {
    return input
        .apply(ParDo.of(new KeyOnDatastreamRowId()))
        .apply(
            ParDo.of(new StatefulCleanDatastreamRowFn(failureTag))
                .withOutputTags(successTag, TupleTagList.of(failureTag)));
  }

  /**
   * The {@code StatefulCleanDatastreamRowFn} class statefully processes and filters data based on
   * the supplied primary keys and sort keys in DmlInfo.
   */
  public static class KeyOnDatastreamRowId extends DoFn<TableRow, KV<String, TableRow>> {

    public KeyOnDatastreamRowId() {}

    @ProcessElement
    public void processElement(ProcessContext context) {
      TableRow tableRow = context.element();
      DatastreamRow row = DatastreamRow.of(tableRow);
      if (row.getSourceType().equals("oracle")) {
        context.output(KV.of(row.getOracleRowId(), tableRow));
      } else {
        String generatedString =
            RandomStringUtils.random(
                /* length */ 10, /* useLetters */ true, /* useNumbers */ false);
        context.output(KV.of(generatedString, tableRow));
      }
    }
  }

  /**
   * The {@code StatefulCleanDatastreamRowFn} class statefully processes and filters data based on
   * the supplied primary keys and sort keys in the TableRow.
   *
   * <p>The core usecase is to ensure Oracle transaction rollbacks are supplemented with the correct
   * primary key values.
   */
  public static class StatefulCleanDatastreamRowFn extends DoFn<KV<String, TableRow>, TableRow> {

    public final TupleTag<TableRow> failureTag;
    private static final String PK_STATE_ID = "pk-state-id";
    private final Distribution distribution =
        Metrics.distribution(StatefulCleanDatastreamRowFn.class, "replicationDistribution");

    @StateId(PK_STATE_ID)
    private final StateSpec<ValueState<TableRow>> myStateSpec =
        StateSpecs.value(TableRowJsonCoder.of());

    public StatefulCleanDatastreamRowFn(TupleTag<TableRow> failureTag) {
      this.failureTag = failureTag;
    }

    @ProcessElement
    public void processElement(
        ProcessContext context, @StateId(PK_STATE_ID) ValueState<TableRow> myState) {
      TableRow tableRow = context.element().getValue();
      DatastreamRow row = DatastreamRow.of(tableRow);
      if (!row.getSourceType().equals("oracle")) {
        context.output(tableRow);
        return;
      }

      List<String> primaryKeys = row.getPrimaryKeys();
      // If the there is no PK or it is ROWID, nothing needs the be done.
      if (primaryKeys.isEmpty()
          || primaryKeys.equals(ImmutableList.of(DatastreamRow.DEFAULT_ORACLE_PRIMARY_KEY))) {
        context.output(tableRow);
        return;
      }

      // When Primary Keys are not ROWID, use stateful logic.
      if (hasPrimaryKeyValues(tableRow, primaryKeys)) {
        myState.write(getPrimaryKeysTableRow(tableRow, primaryKeys));
        context.output(tableRow);
        return;
      }

      TableRow previousTableRow = myState.read();
      if (previousTableRow == null) {
        LOG.warn(
            "Failed stateful clean requires manual attention for ROWID: {}",
            tableRow.get(DatastreamRow.DEFAULT_ORACLE_PRIMARY_KEY));
        context.output(failureTag, tableRow);
        return;
      }
      TableRow newTableRow = tableRow.clone();
      for (String primaryKey : primaryKeys) {
        newTableRow.put(primaryKey, previousTableRow.get(primaryKey));
      }

      context.output(newTableRow);
    }

    private TableRow getPrimaryKeysTableRow(TableRow tableRow, List<String> primaryKeys) {
      TableRow pkRow = new TableRow();

      for (String primaryKey : primaryKeys) {
        pkRow.set(primaryKey, tableRow.get(primaryKey));
      }

      return pkRow;
    }

    private Boolean hasPrimaryKeyValues(TableRow tableRow, List<String> primaryKeys) {
      for (String primaryKey : primaryKeys) {
        if (!tableRow.containsKey(primaryKey) || tableRow.get(primaryKey) == null) {
          return false;
        }
      }

      return true;
    }
  }

  /**
   * The RowCleanerDeadLetterQueueSanitizer cleans and prepares failed row cleaner events to be
   * stored in a GCS Dead Letter Queue. NOTE: The input to a Sanitizer is flexible but the output
   * must be a String unless your override formatMessage()
   */
  public static class RowCleanerDeadLetterQueueSanitizer
      extends DeadLetterQueueSanitizer<TableRow, String> {

    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    @Override
    public String getJsonMessage(TableRow row) {
      String message;
      try {
        row.setFactory(JSON_FACTORY);
        message = row.toPrettyString();
      } catch (IOException e) {
        // Ignore exception and print bad format
        message = String.format("\"%s\"", row.toString());
      }

      return message;
    }

    @Override
    public String getErrorMessageJson(TableRow row) {
      return "Failed stateful clean requires manual attention for ROWID";
    }
  }
}
