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

import com.google.cloud.teleport.v2.neo4j.model.sources.TextSource;
import com.google.cloud.teleport.v2.neo4j.utils.BeamUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/** Transform to return zero row PCollection with schema from text sources. */
public class TextSourceFileMetadataToRow extends PTransform<PBegin, PCollection<Row>> {

  private final TextSource source;

  public TextSourceFileMetadataToRow(TextSource source) {
    this.source = source;
  }

  @Override
  public PCollection<Row> expand(PBegin input) {
    Schema schema = BeamUtils.textToBeamSchema(source.getHeader());
    return input.apply(Create.empty(schema));
  }
}
