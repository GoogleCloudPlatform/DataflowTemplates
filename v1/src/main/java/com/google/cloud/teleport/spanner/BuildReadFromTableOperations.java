/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.spanner;

import static com.google.cloud.teleport.spanner.SpannerTableFilter.getFilteredTables;

import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.teleport.spanner.ddl.Column;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.Table;
import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/** Given a Cloud Spanner {@link Ddl} generates a "read all" operation per table. */
class BuildReadFromTableOperations
    extends PTransform<PCollection<Ddl>, PCollection<ReadOperation>> {

  // The number of read partitions has to be capped so that in case the Partition token is large
  // (which can happen with a table with a lot of columns), the PartitionResponse size is bounded.
  private static final int MAX_PARTITIONS = 1000;

  // A list of tables the user intends to export from Cloud Spanner along with their
  // original data; BuildReadFromTableOperations(tables) will create ReadOperations for
  // those tables (along with any necessary parent or foreign key tables). If this list is empty,
  // then ReadOperations will be made for every table in the database.
  private final ValueProvider<String> tables;

  public BuildReadFromTableOperations(ValueProvider<String> tables) {
    this.tables = tables;
  }

  @Override
  public PCollection<ReadOperation> expand(PCollection<Ddl> ddl) {
    return ddl.apply(
        "Read from table operations",
        ParDo.of(
            new DoFn<Ddl, ReadOperation>() {

              @ProcessElement
              public void processElement(ProcessContext c) {
                Ddl ddl = c.element();

                List<String> tablesList = Collections.emptyList();

                // If the user provides a comma-separated list of strings, parse it into a List
                if (!tables.get().trim().isEmpty()) {
                  tablesList = Arrays.asList(tables.get().split(",\\s*"));
                }

                for (Table table : getFilteredTables(ddl, tablesList)) {
                  String columnsListAsString =
                      table.columns().stream()
                          .filter(x -> !x.isGenerated())
                          .map(x -> createColumnExpression(x))
                          .collect(Collectors.joining(","));

                  PartitionOptions partitionOptions =
                      PartitionOptions.newBuilder().setMaxPartitions(MAX_PARTITIONS).build();

                  // Also have to export table name to be able to identify which row belongs to
                  // which table.
                  ReadOperation read;
                  switch (ddl.dialect()) {
                    case GOOGLE_STANDARD_SQL:
                      read =
                          ReadOperation.create()
                              .withQuery(
                                  String.format(
                                      "SELECT \"%s\" AS _spanner_table, %s FROM `%s` AS t",
                                      table.name(), columnsListAsString, table.name()))
                              .withPartitionOptions(partitionOptions);
                      break;
                    case POSTGRESQL:
                      read =
                          ReadOperation.create()
                              .withQuery(
                                  String.format(
                                      "SELECT '%s' AS _spanner_table, %s FROM \"%s\" AS t",
                                      table.name(), columnsListAsString, table.name()))
                              .withPartitionOptions(partitionOptions);
                      break;
                    default:
                      throw new IllegalArgumentException(
                          String.format("Unrecognized dialect: %s", ddl.dialect()));
                  }
                  c.output(read);
                }
              }
            }));
  }

  @VisibleForTesting
  String createColumnExpression(Column col) {
    switch (col.dialect()) {
      case GOOGLE_STANDARD_SQL:
        if (col.typeString().equals("JSON")) {
          return "TO_JSON_STRING(" + "t.`" + col.name() + "`" + ") AS " + col.name();
        }
        if (col.typeString().equals("ARRAY<NUMERIC>")) {
          return "(SELECT ARRAY_AGG(CAST(num AS STRING)) FROM UNNEST("
              + "t.`"
              + col.name()
              + "`"
              + ") AS num) AS "
              + col.name();
        }
        if (col.typeString().equals("ARRAY<JSON>")) {
          return "(SELECT ARRAY_AGG(TO_JSON_STRING(element)) FROM UNNEST("
              + "t.`"
              + col.name()
              + "`"
              + ") AS element) AS "
              + col.name();
        }
        return "t.`" + col.name() + "`";
      case POSTGRESQL:
        return "t.\"" + col.name() + "\"";
      default:
        throw new IllegalArgumentException(
            String.format("Unrecognized dialect: %s", col.dialect()));
    }
  }
}
