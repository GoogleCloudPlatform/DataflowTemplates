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
package com.google.cloud.teleport.v2.transformer;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.cloud.teleport.v2.spanner.migrations.avro.GenericRecordTypeConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import java.io.Serializable;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a DoFn class that takes a {@link SourceRow} and converts it into a {@link
 * com.google.cloud.spanner.Mutation}.
 */
@AutoValue
public abstract class SourceRowToMutationDoFn extends DoFn<SourceRow, Mutation>
    implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(SourceRowToMutationDoFn.class);

  public abstract ISchemaMapper iSchemaMapper();

  public abstract Map<String, SourceTableReference> tableIdMapper();

  public static SourceRowToMutationDoFn create(
      ISchemaMapper iSchemaMapper, Map<String, SourceTableReference> tableIdMapper) {
    return new AutoValue_SourceRowToMutationDoFn(iSchemaMapper, tableIdMapper);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    SourceRow sourceRow = c.element();
    if (!tableIdMapper().containsKey(sourceRow.tableSchemaUUID())) {
      // TODO: Remove LOG statements from processElement once counters and DLQ is supported.
      LOG.error(
          "cannot find valid sourceTable for tableId: {} in tableIdMapper",
          sourceRow.tableSchemaUUID());
      return;
    }
    try {
      // TODO: update namespace in constructor when Spanner namespace support is added.
      GenericRecord record = sourceRow.getPayload();
      String srcTableName = tableIdMapper().get(sourceRow.tableSchemaUUID()).sourceTableName();
      GenericRecordTypeConvertor genericRecordTypeConvertor =
          new GenericRecordTypeConvertor(iSchemaMapper(), "");
      Map<String, Value> values =
          genericRecordTypeConvertor.transformChangeEvent(record, srcTableName);
      String spannerTableName = iSchemaMapper().getSpannerTableName("", srcTableName);
      Mutation mutation = mutationFromMap(spannerTableName, values);
      c.output(mutation);
    } catch (Exception e) {
      // TODO: Add DLQ integration once supported.
      LOG.error(
          "Unable to transform source row to spanner mutation: {} {}",
          e.getMessage(),
          e.fillInStackTrace());
    }
  }

  private static Mutation mutationFromMap(String spannerTableName, Map<String, Value> values) {
    Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(spannerTableName);
    for (String spannerColName : values.keySet()) {
      builder.set(spannerColName).to(values.get(spannerColName));
    }
    return builder.build();
  }
}
