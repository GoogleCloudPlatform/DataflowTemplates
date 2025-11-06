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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
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
  // Avoid serializing tableTransform as it's needed only for expand.
  private transient ImmutableMap<
          ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
      tableTransforms;

  public abstract TupleTag<SourceRow> sourceRowTag();

  public abstract TupleTag<SourceTableReference> sourceTableReferenceTag();

  private AccumulatingTableReader setTableTransforms(
      ImmutableMap<ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
          tableTransforms) {
    this.tableTransforms = tableTransforms;
    return this;
  }

  @Override
  public PCollectionTuple expand(PBegin input) {
    ImmutableMap<ImmutableList<SourceTableReference>, PCollection<SourceRow>> tablePCollections =
        this.tableTransforms.entrySet().stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    Entry::getKey,
                    e -> {
                      String groupName = getTableGroupName(e.getKey());
                      return input.apply("SourceRowReader." + groupName, e.getValue());
                    }));
    PCollection<SourceRow> sourceRowPCollection =
        PCollectionList.of(tablePCollections.values())
            /* Flatten SourceRows read from all the tables into a single PCollection */
            .apply("SourceRowCollector", Flatten.<SourceRow>pCollections());

    PCollection<SourceTableReference> tableCompletions =
        PCollectionList.of(
                tablePCollections.entrySet().stream()
                    .map(
                        e -> {
                          ImmutableList<SourceTableReference> tableReferences = e.getKey();
                          PCollection<SourceRow> groupRows = e.getValue();

                          String groupName = getTableGroupName(tableReferences);

                          // 1. Zero counts for all tables in the group to ensure completion signal
                          // even for empty tables.
                          PCollection<KV<String, Long>> zeroCounts =
                              input
                                  .getPipeline()
                                  .apply(
                                      "CreateZeroCounts." + groupName,
                                      Create.of(
                                          tableReferences.stream()
                                              .map(ref -> KV.of(ref.sourceTableName(), 0L))
                                              .collect(Collectors.toList())))
                                  .setCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()));

                          // 2. Actual counts from the reader.
                          PCollection<KV<String, Long>> actualCounts =
                              groupRows
                                  .apply(
                                      "ExtractTableNames." + groupName,
                                      ParDo.of(new ExtractTableNameFn()))
                                  .apply("CountPerTable." + groupName, Count.perElement());

                          // 3. Aggregate and Emit Completions.
                          return PCollectionList.of(actualCounts)
                              .and(zeroCounts)
                              .apply("FlattenCounts." + groupName, Flatten.pCollections())
                              .apply("SumCounts." + groupName, Combine.perKey(Sum.ofLongs()))
                              .apply(
                                  "GenerateCompletions." + groupName,
                                  ParDo.of(new GroupCompletionDoFn(tableReferences)));
                        })
                    .collect(ImmutableList.toImmutableList()))
            /* Flatten Completions of all the tables into a single PCollection */
            .apply("TableCompletionCollector", Flatten.<SourceTableReference>pCollections());
    return PCollectionTuple.of(this.sourceRowTag(), sourceRowPCollection)
        .and(this.sourceTableReferenceTag(), tableCompletions);
  }

  @VisibleForTesting
  protected static String getTableGroupName(ImmutableList<SourceTableReference> tableReferences) {
    String groupName =
        tableReferences.stream()
            .map(SourceTableReference::getName)
            .collect(Collectors.joining("_"));
    if (groupName.length() > 100) {
      String hash = String.valueOf(groupName.hashCode());
      // We want to keep some part of the original name but ensure the total length is <= 100.
      // Total = substring + "_" + hash.
      // substring length = 100 - 1 - hash.length().
      int maxSubstringLength = 100 - 1 - hash.length();
      groupName = groupName.substring(0, maxSubstringLength) + "_" + hash;
    }
    return groupName;
  }

  /**
   * DoFn to extract the table name from a {@link SourceRow} and delimit it to match the format used
   * in {@link SourceTableReference}.
   */
  private static class ExtractTableNameFn extends DoFn<SourceRow, String> {
    @ProcessElement
    public void processElement(@Element SourceRow row, OutputReceiver<String> out) {
      out.output(delimitIdentifier(row.tableName()));
    }

    private String delimitIdentifier(String identifier) {
      return "\"" + identifier.replaceAll("\"", "\"\"") + "\"";
    }
  }

  /**
   * DoFn to map the aggregated record counts for each table back to their corresponding {@link
   * SourceTableReference} and output them with the updated count.
   */
  private static class GroupCompletionDoFn extends DoFn<KV<String, Long>, SourceTableReference> {
    private final Map<String, SourceTableReference> tableReferencesMap;

    public GroupCompletionDoFn(ImmutableList<SourceTableReference> tableReferences) {
      this.tableReferencesMap =
          tableReferences.stream()
              .collect(Collectors.toMap(SourceTableReference::sourceTableName, ref -> ref));
    }

    @ProcessElement
    public void processElement(
        @Element KV<String, Long> element, OutputReceiver<SourceTableReference> out) {
      SourceTableReference ref = tableReferencesMap.get(element.getKey());
      if (ref != null) {
        out.output(ref.toBuilder().setRecordCount(element.getValue()).build());
      }
    }
  }

  static Builder builder(
      TupleTag<SourceRow> sourceRowTag, TupleTag<SourceTableReference> sourceTableReferenceTag) {
    Builder builder = new AutoValue_AccumulatingTableReader.Builder();
    builder.setSourceRowTag(sourceRowTag).setSourceTableReferenceTag(sourceTableReferenceTag);
    return builder;
  }

  @AutoValue.Builder
  abstract static class Builder {

    private ImmutableMap.Builder<
            ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
        tableTransformsBuilder = new ImmutableMap.Builder<>();

    abstract Builder setSourceRowTag(TupleTag<SourceRow> sourceRowTag);

    abstract Builder setSourceTableReferenceTag(TupleTag<SourceTableReference> sourceRowTag);

    Builder withTableReader(
        ImmutableList<SourceTableReference> sourceTableReferences,
        PTransform<PBegin, PCollection<SourceRow>> tableReader) {
      this.tableTransformsBuilder.put(sourceTableReferences, tableReader);
      return this;
    }

    abstract AccumulatingTableReader autoBuild();

    AccumulatingTableReader build() {
      return autoBuild().setTableTransforms(this.tableTransformsBuilder.build());
    }
  }
}
