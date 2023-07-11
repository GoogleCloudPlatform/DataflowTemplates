/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.providers.text;

import com.google.cloud.teleport.v2.neo4j.model.helpers.SourceQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transform that reads data from a source file. */
public class TextSourceFileToRow extends PTransform<PBegin, PCollection<Row>> {

  private static final Logger LOG = LoggerFactory.getLogger(TextSourceFileToRow.class);
  SourceQuerySpec sourceQuerySpec;

  public TextSourceFileToRow(SourceQuerySpec sourceQuerySpec) {
    this.sourceQuerySpec = sourceQuerySpec;
  }

  @Override
  public PCollection<Row> expand(PBegin input) {
    Source source = sourceQuerySpec.getSource();
    Schema beamTextSchema = sourceQuerySpec.getSourceSchema();
    String dataFileUri = source.getUri();

    if (StringUtils.isNotBlank(dataFileUri)) {
      LOG.info("Ingesting file: {}.", dataFileUri);
      return input
          .apply(
              "Read " + source.getName() + " data: " + dataFileUri, TextIO.read().from(dataFileUri))
          .apply(
              "Parse lines into string columns.",
              ParDo.of(new LineToRowFn(source, beamTextSchema, source.getCsvFormat())))
          .setRowSchema(beamTextSchema);
    } else if (source.getInline() != null && !source.getInline().isEmpty()) {
      LOG.info("Processing {} rows inline.", source.getInline().size());
      return input
          .apply("Ingest inline dataset: " + source.getName(), Create.of(source.getInline()))
          .apply(
              "Parse lines into string columns.", ParDo.of(new ListOfStringToRowFn(beamTextSchema)))
          .setRowSchema(beamTextSchema);
    } else {
      throw new RuntimeException("Data not found.");
    }
  }
}
