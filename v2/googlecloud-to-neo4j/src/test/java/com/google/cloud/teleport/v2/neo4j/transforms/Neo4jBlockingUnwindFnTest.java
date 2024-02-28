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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.telemetry.ReportedSourceType;
import com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.neo4j.driver.TransactionConfig;

public class Neo4jBlockingUnwindFnTest {

  @Test
  public void sendsTransactionMetadata() {
    Neo4jConnection connection = mock(Neo4jConnection.class);
    Neo4jBlockingUnwindFn batchImporter =
        new Neo4jBlockingUnwindFn(
            ReportedSourceType.BIGQUERY,
            TargetType.edge,
            "RETURN 42",
            false,
            "map",
            (row) -> DataCastingUtils.rowToNeo4jDataMap(row, mock(Target.class)),
            () -> connection);

    batchImporter.setup();
    batchImporter.processElement(aProcessContext());

    Map<String, String> expectedTxMetadata =
        Map.of("sink", "neo4j", "source", "BigQuery", "target-type", "edge", "step", "import");
    TransactionConfig expectedTransactionConfig =
        TransactionConfig.builder()
            .withMetadata(Map.of("app", "dataflow", "metadata", expectedTxMetadata))
            .build();
    verify(connection).writeTransaction(any(), eq(expectedTransactionConfig));
  }

  private static DoFn.ProcessContext aProcessContext() {
    ProcessContext context = mock(ProcessContext.class);
    Row row = mock(Row.class, RETURNS_DEEP_STUBS);
    when(context.element()).thenReturn(KV.of(42, List.of(row)));
    return context;
  }
}
