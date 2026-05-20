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

import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.mapper.ComparisonRecordMapper;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import java.util.Objects;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionView;

/** A {@link DoFn} that converts {@link GenericRecord} to {@link ComparisonRecord}. */
public class SourceHashFn extends DoFn<GenericRecord, ComparisonRecord> {

  private final PCollectionView<Ddl> ddlView;
  private final SerializableFunction<Ddl, ISchemaMapper> schemaMapperProvider;

  private transient ComparisonRecordMapper comparisonRecordMapper;

  public SourceHashFn(
      PCollectionView<Ddl> ddlView, SerializableFunction<Ddl, ISchemaMapper> schemaMapperProvider) {
    this.ddlView = ddlView;
    this.schemaMapperProvider = schemaMapperProvider;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    Ddl ddl = c.sideInput(ddlView);

    // lazy initialization of the mapper.
    if (comparisonRecordMapper == null) {
      comparisonRecordMapper =
          new ComparisonRecordMapper(schemaMapperProvider.apply(ddl), null, ddl);
    }

    ComparisonRecord comparisonRecord =
        comparisonRecordMapper.mapFrom(Objects.requireNonNull(c.element()));
    if (comparisonRecord != null) {
      c.output(comparisonRecord);
    }
  }
}
