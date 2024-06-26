/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.source.reader.io.transform.ReaderTransform;
import java.io.Serializable;

@AutoValue
public abstract class ReaderImpl implements Reader, Serializable {

  abstract SourceSchema sourceSchema();

  abstract ReaderTransform readerTransform();

  public static ReaderImpl of(IoWrapper ioWrapper) {
    SourceSchema sourceSchema = ioWrapper.discoverTableSchema();
    ReaderTransform.Builder readerTransformBuilder = ReaderTransform.builder();
    ioWrapper
        .getTableReaders()
        .entrySet()
        .forEach(entry -> readerTransformBuilder.withTableReader(entry.getKey(), entry.getValue()));

    return ReaderImpl.create(sourceSchema, readerTransformBuilder.build());
  }

  @Override
  public SourceSchema getSourceSchema() {
    return this.sourceSchema();
  }

  @Override
  public ReaderTransform getReaderTransform() {
    return this.readerTransform();
  }

  static ReaderImpl create(SourceSchema sourceSchema, ReaderTransform readerTransform) {
    return new AutoValue_ReaderImpl(sourceSchema, readerTransform);
  }
}
