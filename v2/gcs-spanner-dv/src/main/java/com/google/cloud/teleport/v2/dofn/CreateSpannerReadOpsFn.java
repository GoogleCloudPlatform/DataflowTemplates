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
package com.google.cloud.teleport.v2.dofn;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

public class CreateSpannerReadOpsFn extends DoFn<Void, ReadOperation> {

  private final PCollectionView<Ddl> ddlView;

  public CreateSpannerReadOpsFn(PCollectionView<Ddl> ddlView) {
    this.ddlView = ddlView;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    Ddl ddl = c.sideInput(ddlView);
    List<String> tableNames = ddl.getTablesOrderedByReference();
    tableNames.forEach(
        tableName -> {
          // We encode the tableName in the query itself to push table information dynamically
          // and avoid table level stages.
          String query =
              String.format("SELECT *, '%s' as __tableName__ FROM %s", tableName, tableName);
          c.output(ReadOperation.create().withQuery(query));
        });
  }
}
