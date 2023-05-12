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
package com.google.cloud.teleport.v2.neo4j.model.helpers;

import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import org.apache.beam.sdk.schemas.Schema;

/** Convenience object for passing Source metadata and PCollection schema together. */
public class SourceQuerySpec {

  private final Source source;
  private final Schema sourceSchema;

  public SourceQuerySpec(Source source, Schema sourceSchema) {
    this.source = source;
    this.sourceSchema = sourceSchema;
  }

  public Source getSource() {
    return source;
  }

  public Schema getSourceSchema() {
    return sourceSchema;
  }

  public static class SourceQuerySpecBuilder {

    private Source source;
    private Schema sourceSchema;

    public SourceQuerySpecBuilder source(Source source) {
      this.source = source;
      return this;
    }

    public SourceQuerySpecBuilder sourceSchema(Schema sourceSchema) {
      this.sourceSchema = sourceSchema;
      return this;
    }

    public SourceQuerySpec build() {
      return new SourceQuerySpec(source, sourceSchema);
    }
  }
}
