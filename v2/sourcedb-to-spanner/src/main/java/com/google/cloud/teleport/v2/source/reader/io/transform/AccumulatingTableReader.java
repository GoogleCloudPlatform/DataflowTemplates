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
package com.google.cloud.teleport.v2.source.reader.io.transform;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map.Entry;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Helper to {@link ReaderTransform} that expands the Pipeline Graph to flatten all the tableReaders
 * and generate table completions. Please refer to {@link ReaderTransform} for greater details.
 */
@AutoValue
abstract class AccumulatingTableReader extends PTransform<PBegin, PCollectionTuple> {
  public abstract ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>>
      tableTransforms();

  public abstract TupleTag<SourceRow> sourceRowTag();

  public abstract TupleTag<SourceTableReference> sourceTableReferenceTag();

  @Override
  public PCollectionTuple expand(PBegin input) {
    ImmutableMap<SourceTableReference, PCollection<SourceRow>> tablePCollections =
        this.tableTransforms().entrySet().stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    Entry::getKey,
                    e -> input.apply("SourceRowReader." + e.getKey().getName(), e.getValue())));
    PCollection<SourceRow> sourceRowPCollection =
        PCollectionList.of(tablePCollections.values())
            /* Flatten SourceRows read from all the tables into a single PCollection */
            .apply("SourceRowCollector", Flatten.<SourceRow>pCollections());

    PCollection<SourceTableReference> tableCompletions =
        PCollectionList.of(
                tablePCollections.entrySet().stream()
                    .map(
                        e ->
                            /*
                             * Reduce the Collection of SourceRows to their counts in a parallel stage.
                             * This ensures that this parallel stage waits till the table is completed,
                             * allowing the reader to signal completion of reading a table to the pipeline controller.
                             * As needed, more complex reduce operation can be performed providing additional information to the pipelineController.
                             * TODO(vardhanvthigle): Add a note on cost vs performance of the Count transform after benchmarking.
                             */
                            e.getValue()
                                .apply("TableRowCounter." + e.getKey().getName(), Count.globally())
                                .apply(
                                    "TableCompletion." + e.getKey().getName(),
                                    MapElements.via(new SourceTableReferenceWithCount(e.getKey()))))
                    .collect(ImmutableList.toImmutableList()))
            /* Flatten Completions of all the tables into a single PCollection */
            .apply("TableCompletionCollector", Flatten.<SourceTableReference>pCollections());
    return PCollectionTuple.of(this.sourceRowTag(), sourceRowPCollection)
        .and(this.sourceTableReferenceTag(), tableCompletions);
  }

  static Builder builder(
      TupleTag<SourceRow> sourceRowTag, TupleTag<SourceTableReference> sourceTableReferenceTag) {
    Builder builder = new AutoValue_AccumulatingTableReader.Builder();
    builder.setSourceRowTag(sourceRowTag).setSourceTableReferenceTag(sourceTableReferenceTag);
    return builder;
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract ImmutableMap.Builder tableTransformsBuilder();

    abstract Builder setSourceRowTag(TupleTag<SourceRow> sourceRowTag);

    abstract Builder setSourceTableReferenceTag(TupleTag<SourceTableReference> sourceRowTag);

    Builder withTableReader(
        SourceTableReference sourceTableReference,
        PTransform<PBegin, PCollection<SourceRow>> tableReader) {
      this.tableTransformsBuilder().put(sourceTableReference, tableReader);
      return this;
    }

    abstract AccumulatingTableReader build();
  }

  public class SourceTableReferenceWithCount extends SimpleFunction<Long, SourceTableReference>
      implements SerializableFunction<Long, SourceTableReference> {
    private SourceTableReference tableReference;

    SourceTableReferenceWithCount(SourceTableReference tableReference) {
      this.tableReference = tableReference;
    }

    @Override
    public SourceTableReference apply(Long input) {
      return this.tableReference.toBuilder().setRecordCount(input).build();
    }
  }
}
