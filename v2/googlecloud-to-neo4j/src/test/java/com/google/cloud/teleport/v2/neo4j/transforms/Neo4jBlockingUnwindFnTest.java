/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.transforms;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.telemetry.ReportedSourceType;
import com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils;
import com.google.cloud.teleport.v2.neo4j.utils.WriteStats;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.SummaryCounters;
import org.neo4j.importer.v1.pipeline.CustomQueryTargetStep;
import org.neo4j.importer.v1.pipeline.TargetStep;

public class Neo4jBlockingUnwindFnTest {

  @Test
  public void sends_transaction_metadata() {
    var connection = mock(Neo4jConnection.class);
    var summary = mock(ResultSummary.class);
    when(summary.counters()).thenReturn(mock(SummaryCounters.class));
    doReturn(summary).when(connection).writeTransaction(any(), any());
    var batchImporter = newImportFn(newQueryStep("a-query-step", "RETURN 42"), connection);

    batchImporter.setup();
    batchImporter.processElement(aProcessContext());

    Map<String, String> expectedTxMetadata =
        Map.of("sink", "neo4j", "source", "BigQuery", "target-type", "query", "step", "import");
    TransactionConfig expectedTransactionConfig =
        TransactionConfig.builder()
            .withMetadata(Map.of("app", "dataflow", "metadata", expectedTxMetadata))
            .build();
    verify(connection).writeTransaction(any(), eq(expectedTransactionConfig));
  }

  @Test
  public void emits_write_statistics_for_each_transaction() throws Exception {
    var connection = mock(Neo4jConnection.class);
    var summary = mock(ResultSummary.class);
    var counters = mock(SummaryCounters.class);
    when(summary.counters()).thenReturn(counters);
    when(counters.containsUpdates()).thenReturn(true);
    when(counters.nodesCreated()).thenReturn(2);
    when(counters.propertiesSet()).thenReturn(5);
    doReturn(summary).when(connection).writeTransaction(any(), any());
    var batchImporter = newImportFn(newQueryStep("a-query-step", "RETURN 42"), connection);

    batchImporter.setup();
    var context = aProcessContext();
    batchImporter.processElement(context);

    var captor = ArgumentCaptor.forClass(Row.class);
    verify(context).output(captor.capture());
    var stats = captor.getValue();
    assertThat(stats.getString("step")).isEqualTo("a-query-step");
    assertThat(stats.getInt64("batch_rows")).isEqualTo(1L);
    assertThat(stats.getBoolean("contains_updates")).isTrue();
    assertThat(stats.getBoolean("contains_system_updates")).isFalse();
    assertThat(stats.getInt64("nodes_created")).isEqualTo(2L);
    assertThat(stats.getInt64("properties_set")).isEqualTo(5L);
    assertThat(stats.getInt64("relationships_created")).isEqualTo(0L);
    // The emitted Row must be encodable with the schema the transform sets on the output.
    CoderUtils.encodeToByteArray(WriteStats.coder(), stats);
  }

  @Test
  public void does_not_emit_statistics_for_empty_batch() {
    var connection = mock(Neo4jConnection.class);
    var batchImporter = newImportFn(newQueryStep("a-query-step", "RETURN 42"), connection);
    var context = mock(DoFn.ProcessContext.class);
    when(context.element()).thenReturn(KV.of(42, List.<Row>of()));

    batchImporter.processElement(context);

    verify(context, never()).output(any());
  }

  private static Neo4jBlockingUnwindFn newImportFn(TargetStep step, Neo4jConnection connection) {
    return new Neo4jBlockingUnwindFn(
        ReportedSourceType.BIGQUERY,
        step,
        false,
        "map",
        DataCastingUtils::rowToNeo4jDataMap,
        () -> connection);
  }

  private static CustomQueryTargetStep newQueryStep(String name, String query) {
    var mock = mock(CustomQueryTargetStep.class);
    when(mock.name()).thenReturn(name);
    when(mock.query()).thenReturn(query);
    return mock;
  }

  private static DoFn.ProcessContext aProcessContext() {
    var context = mock(ProcessContext.class);
    var row = mock(Row.class, RETURNS_DEEP_STUBS);
    when(context.element()).thenReturn(KV.of(42, List.of(row)));
    return context;
  }
}
