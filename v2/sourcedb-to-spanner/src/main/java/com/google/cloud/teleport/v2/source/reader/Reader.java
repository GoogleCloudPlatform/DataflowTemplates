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

import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.source.reader.io.transform.ReaderTransform;

/**
 * The Reader interfaces with the source-db allowing the pipeline to read from various data sources
 * like jdbc compliant databases for example.
 *
 * <p>The Reader provides functionality to the pipeline to read the source schema, and, get the
 * ReaderTransform. The ReaderTransform typically sits close to the pipeline root and returns two
 * PCollections in a tagged-tuple output:
 *
 * <ol>
 *   <li><code>PCollection&lt;SourceRows&gt;</code> - The Sources rows of all the tables that reader
 *       is reading.
 *   <li><code>PCollection&lt;SourceTableReference&gt;</code> - A reference to tables that have
 *       completed their read.
 * </ol>
 *
 * <p>The PipelineController will chain the SourceRow PCollection to the transformer followed by
 * spannerWriter. The table completions can be used to checkpoint the migration, emit an event to a
 * downstream pipeline or anything else that the pipeline controller would like to make use of from
 * a signal that the pipeline has finished reading a given table.
 *
 * <p><b>Example for pipelineController usage </b>
 *
 * <pre><code>
 *   Reader reader = Reader.builder().set(..).set(..).build();
 *   ReaderTransform readerTransform = reader.getTransform();
 *   Pipeline p = Pipeline.create();
 *   PCollection&lt;pCollectionTuple&gt; pcs = p.apply(readerTransform);
 *   // Apply this to transformer and spannerWriter.
 *   PCollection&lt;SourceRows&gt; = pcs.get(readerTransform.sourceRowTag());
 *   // Use this to take action on table completions
 *   PCollection&lt;SourceTableReference&gt; = pcs.get(readerTransform.sourceTableReferenceTag());
 *   </code></pre>
 *
 * <p>A Note on implementation:
 *
 * <ol>
 *   <li>As per the current architecture design, Reader is at the level of a logical database.
 *   <li>There's a slight syntactic choice between the reader being an interface that returns a
 *       {@code PTransform} or being an abstract class that extends it. We have leaned on the former
 *       for neater implementation and to have a similar implementation pattern followed by other
 *       classes like {@code JDBCIo}, which returns a PTransform via `readWithPartitions`.
 * </ol>
 */
public interface Reader {
  SourceSchema getSourceSchema();

  ReaderTransform getReaderTransform();
}
