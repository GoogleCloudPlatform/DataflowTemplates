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

import com.google.cloud.teleport.v2.neo4j.model.helpers.CsvSources;
import com.google.cloud.teleport.v2.neo4j.model.sources.ExternalTextSource;
import com.google.cloud.teleport.v2.neo4j.model.sources.InlineTextSource;
import com.google.cloud.teleport.v2.neo4j.model.sources.TextSource;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transform that reads data from a source file. */
public class TextSourceFileToRow extends PTransform<PBegin, PCollection<Row>> {

  private static final Logger LOG = LoggerFactory.getLogger(TextSourceFileToRow.class);
  private final TextSource source;
  private final Schema schema;

  public TextSourceFileToRow(TextSource source, Schema schema) {
    this.source = source;
    this.schema = schema;
  }

  @Override
  public PCollection<Row> expand(PBegin input) {
    if (source instanceof ExternalTextSource) {
      ExternalTextSource externalTextSource = (ExternalTextSource) source;
      List<String> urls = externalTextSource.getUrls();

      return PCollectionList.of(
              urls.stream()
                  .map(
                      (url) ->
                          input
                              .apply(
                                  "Read " + source.getName() + " data: " + url,
                                  TextIO.read().from(url))
                              .apply(
                                  "Parse lines into string columns.",
                                  ParDo.of(
                                      new LineToRowFn(
                                          schema,
                                          CsvSources.toCsvFormat(externalTextSource.getFormat()))))
                              .setRowSchema(schema))
                  .collect(Collectors.toList()))
          .apply("Combine all " + source.getName() + " data", Flatten.pCollections());
    }
    if (source instanceof InlineTextSource) {
      InlineTextSource inlineTextSource = (InlineTextSource) source;
      List<List<Object>> rows = inlineTextSource.getData();
      LOG.info("Processing {} rows inline.", rows.size());
      return input
          .apply("Ingest inline dataset: " + source.getName(), Create.of(rows))
          .apply("Parse lines into string columns.", ParDo.of(new ListOfStringToRowFn(schema)))
          .setRowSchema(schema);
    }
    throw new RuntimeException(String.format("Unsupported text source: %s", source.getClass()));
  }
}
