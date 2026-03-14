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
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.coders.GenericRecordCoder;
import com.google.cloud.teleport.v2.dofn.SourceHashFn;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.fn.IdentityGenericRecordFn;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.jetbrains.annotations.NotNull;

public class SourceReaderTransform
    extends PTransform<@NotNull PBegin, @NotNull PCollection<ComparisonRecord>> {

  private final String gcsInputDirectory;
  private final PCollectionView<Ddl> ddlView;
  private final SerializableFunction<Ddl, ISchemaMapper> schemaMapperProvider;

  public SourceReaderTransform(
      String gcsInputDirectory,
      PCollectionView<Ddl> ddlView,
      SerializableFunction<Ddl, ISchemaMapper> schemaMapperProvider) {
    this.gcsInputDirectory = gcsInputDirectory;
    this.ddlView = ddlView;
    this.schemaMapperProvider = schemaMapperProvider;
  }

  @Override
  public @NotNull PCollection<ComparisonRecord> expand(PBegin input) {
    return input
        .apply(
            "ReadSourceAvroRecords",
            AvroIO.parseGenericRecords(new IdentityGenericRecordFn())
                .from(createAvroFilePattern(gcsInputDirectory))
                .withCoder(GenericRecordCoder.of())
                .withHintMatchesManyFiles())
        .apply(
            "CalculateSourceRecordsHash",
            ParDo.of(new SourceHashFn(ddlView, schemaMapperProvider)).withSideInputs(ddlView));
  }

  private static String createAvroFilePattern(String inputPath) {
    String cleanPath =
        inputPath.endsWith("/") ? inputPath.substring(0, inputPath.length() - 1) : inputPath;
    return cleanPath + "/**.avro";
  }
}
