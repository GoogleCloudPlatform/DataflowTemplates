/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.dofn;

import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.SchemaConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DoFn to apply schema overrides from a HOCON/JSON config file. */
public class ApplyOverridesFn extends DoFn<DataGeneratorSchema, DataGeneratorSchema> {
  private static final Logger LOG = LoggerFactory.getLogger(ApplyOverridesFn.class);

  private final SchemaConfig schemaConfig;
  private final int defaultInsertQps;
  private final int defaultUpdateQps;
  private final int defaultDeleteQps;

  public ApplyOverridesFn(
      SchemaConfig schemaConfig,
      Integer defaultInsertQps,
      Integer defaultUpdateQps,
      Integer defaultDeleteQps) {
    this.schemaConfig = schemaConfig;
    this.defaultInsertQps = defaultInsertQps;
    this.defaultUpdateQps = defaultUpdateQps;
    this.defaultDeleteQps = defaultDeleteQps;
  }

  @ProcessElement
  public void processElement(
      @Element DataGeneratorSchema schema, OutputReceiver<DataGeneratorSchema> receiver) {
    Map<String, DataGeneratorTable> tableMap = new HashMap<>();

    // Step 1: Apply global defaults
    for (DataGeneratorTable table : schema.tables().values()) {
      tableMap.put(
          table.name(),
          table.toBuilder()
              .insertQps(defaultInsertQps)
              .updateQps(defaultUpdateQps)
              .deleteQps(defaultDeleteQps)
              .build());
    }

    // Step 2: Apply overrides
    if (schemaConfig != null) {
      if (schemaConfig.getTables() != null) {
        for (Map.Entry<String, SchemaConfig.TableConfig> entry :
            schemaConfig.getTables().entrySet()) {
          String tableName = entry.getKey();
          SchemaConfig.TableConfig tableConfig = entry.getValue();
          DataGeneratorTable existingTable = tableMap.get(tableName);
          if (existingTable == null) {
            LOG.warn("Override specified for unknown table: {}", tableName);
            continue;
          }
          DataGeneratorTable.Builder tableBuilder = existingTable.toBuilder();
          if (tableConfig.getInsertQps() != null) {
            tableBuilder.insertQps(tableConfig.getInsertQps());
          }
          if (tableConfig.getUpdateQps() != null) {
            tableBuilder.updateQps(tableConfig.getUpdateQps());
          }
          if (tableConfig.getDeleteQps() != null) {
            tableBuilder.deleteQps(tableConfig.getDeleteQps());
          }
          if (tableConfig.getColumns() != null) {
            tableBuilder.columns(applyColumnOverrides(existingTable, tableConfig.getColumns()));
          }
          if (tableConfig.getForeignKeys() != null) {
            tableBuilder.foreignKeys(mergeForeignKeys(existingTable, tableConfig.getForeignKeys()));
          }
          tableMap.put(tableName, tableBuilder.build());
        }
      }
    }

    receiver.output(
        DataGeneratorSchema.builder()
            .tables(com.google.common.collect.ImmutableMap.copyOf(tableMap))
            .build());
  }

  private com.google.common.collect.ImmutableList<DataGeneratorColumn> applyColumnOverrides(
      DataGeneratorTable existingTable, Map<String, SchemaConfig.ColumnConfig> columnsConfig) {
    List<DataGeneratorColumn> updatedColumns = new ArrayList<>();
    for (DataGeneratorColumn col : existingTable.columns()) {
      if (!columnsConfig.containsKey(col.name())) {
        updatedColumns.add(col);
        continue;
      }
      SchemaConfig.ColumnConfig colConfig = columnsConfig.get(col.name());
      DataGeneratorColumn.Builder colBuilder = col.toBuilder();

      if (colConfig.getFakerExpression() != null) {
        colBuilder.fakerExpression(colConfig.getFakerExpression());
      }
      if (colConfig.getSkip() != null) {
        boolean skip = colConfig.getSkip();
        if (skip && col.isPrimaryKey()) {
          throw new IllegalArgumentException(
              "Cannot skip primary-key column '"
                  + col.name()
                  + "' in table '"
                  + existingTable.name()
                  + "': PK values are required for state-keying and lifecycle events.");
        }
        colBuilder.isSkipped(skip);
      }
      updatedColumns.add(colBuilder.build());
    }
    return com.google.common.collect.ImmutableList.copyOf(updatedColumns);
  }

  private com.google.common.collect.ImmutableList<DataGeneratorForeignKey> mergeForeignKeys(
      DataGeneratorTable existingTable, List<SchemaConfig.ForeignKeyConfig> fkList) {
    LinkedHashMap<String, DataGeneratorForeignKey> byName = new LinkedHashMap<>();
    for (DataGeneratorForeignKey fk : existingTable.foreignKeys()) {
      byName.put(fk.name(), fk);
    }

    for (SchemaConfig.ForeignKeyConfig fkConfig : fkList) {
      String fkName = fkConfig.getName();
      String referencedTable = fkConfig.getReferencedTable();
      List<String> keyCols = fkConfig.getKeyColumns();
      List<String> refCols = fkConfig.getReferencedColumns();

      DataGeneratorForeignKey configured =
          DataGeneratorForeignKey.builder()
              .name(fkName)
              .referencedTable(referencedTable)
              .keyColumns(com.google.common.collect.ImmutableList.copyOf(keyCols))
              .referencedColumns(com.google.common.collect.ImmutableList.copyOf(refCols))
              .build();

      DataGeneratorForeignKey discovered = byName.get(fkName);
      if (discovered != null && !fkEquivalent(discovered, configured)) {
        throw new IllegalArgumentException(
            "Foreign key '"
                + fkName
                + "' on table '"
                + existingTable.name()
                + "' conflicts with the discovered definition. discovered=[refTable="
                + discovered.referencedTable()
                + ", keyColumns="
                + discovered.keyColumns()
                + ", referencedColumns="
                + discovered.referencedColumns()
                + "], config=[refTable="
                + configured.referencedTable()
                + ", keyColumns="
                + configured.keyColumns()
                + ", referencedColumns="
                + configured.referencedColumns()
                + "]. Align the config with the source schema or rename the FK.");
      }
      byName.put(fkName, configured);
    }
    return com.google.common.collect.ImmutableList.copyOf(byName.values());
  }

  private static boolean fkEquivalent(DataGeneratorForeignKey a, DataGeneratorForeignKey b) {
    return a.referencedTable().equals(b.referencedTable())
        && a.keyColumns().equals(b.keyColumns())
        && a.referencedColumns().equals(b.referencedColumns());
  }
}
