/*
 * Copyright (C) 2018 Google Inc.
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

package com.google.cloud.teleport.spanner;

import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.Table;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/** Given a Cloud Spanner {@link Ddl} generates a "read all" operation per table. */
class BuildReadFromTableOperations
    extends PTransform<PCollection<Ddl>, PCollection<ReadOperation>> {

  @Override
  public PCollection<ReadOperation> expand(PCollection<Ddl> ddl) {
    return ddl.apply(
        "Read from table operations",
        ParDo.of(
            new DoFn<Ddl, ReadOperation>() {

              @ProcessElement
              public void processElement(ProcessContext c) {
                Ddl ddl = c.element();
                for (Table table : ddl.allTables()) {
                  String columnsListAsString =
                      table.columns().stream()
                          .map(x -> "t.`" + x.name() + "`")
                          .collect(Collectors.joining(","));
                  // Also have to export table name to be able to identify which row belongs to
                  // which table.
                  ReadOperation read =
                      ReadOperation.create()
                          .withQuery(
                              String.format(
                                  "SELECT \"%s\" AS _spanner_table, %s FROM `%s` AS t",
                                  table.name(), columnsListAsString, table.name()));
                  c.output(read);
                }
              }
            }));
  }
}
