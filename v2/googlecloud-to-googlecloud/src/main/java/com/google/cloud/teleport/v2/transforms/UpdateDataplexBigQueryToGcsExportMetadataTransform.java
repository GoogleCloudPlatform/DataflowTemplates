/*
 * Copyright (C) 2021 Google LLC
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

import com.google.cloud.teleport.v2.values.BigQueryTable;
import com.google.cloud.teleport.v2.values.BigQueryTablePartition;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Updates Dataplex Metadata for the files generated as a result of a BigQuery->Cloud Storage
 * export.
 *
 * <p>The input collection should contain key/value pairs of the source BigQuery tables/partitions
 * mapped to the generated file names in Cloud Storage.
 */
public class UpdateDataplexBigQueryToGcsExportMetadataTransform
    extends PTransform<
        PCollection<KV<BigQueryTable, KV<BigQueryTablePartition, String>>>, PCollection<Void>> {

  private static final Logger LOG =
      LoggerFactory.getLogger(UpdateDataplexBigQueryToGcsExportMetadataTransform.class);

  @Override
  public PCollection<Void> expand(
      PCollection<KV<BigQueryTable, KV<BigQueryTablePartition, String>>> input) {

    return input
        .apply("CombineMetadata", Combine.globally(new AccumulateEntityMapFn()))
        .apply("UpdateDataplex", ParDo.of(new CallDataplexFn()));
  }

  private static class CallDataplexFn
      extends DoFn<Map<BigQueryTable, Map<BigQueryTablePartition, Set<String>>>, Void> {

    @ProcessElement
    public void processElement(
        @Element Map<BigQueryTable, Map<BigQueryTablePartition, Set<String>>> input,
        PipelineOptions options) {

      if (!options.as(Options.class).getUpdateDataplexMetadata()) {
        LOG.info("Skipping Dataplex Metadata update.");
      } else {
        // TODO(an2x): implement Dataplex API call.
        LOG.warn(
            "Would've updated Dataplex Metadata, but not implemented yet. Files created:\n{}",
            input);
      }
    }
  }

  private static class AccumulateEntityMapFn
      extends CombineFn<
          KV<BigQueryTable, KV<BigQueryTablePartition, String>>,
          Map<BigQueryTable, Map<BigQueryTablePartition, Set<String>>>,
          Map<BigQueryTable, Map<BigQueryTablePartition, Set<String>>>> {

    @Override
    public Map<BigQueryTable, Map<BigQueryTablePartition, Set<String>>> createAccumulator() {
      return new HashMap<>();
    }

    @Override
    public Map<BigQueryTable, Map<BigQueryTablePartition, Set<String>>> addInput(
        Map<BigQueryTable, Map<BigQueryTablePartition, Set<String>>> mutableAccumulator,
        KV<BigQueryTable, KV<BigQueryTablePartition, String>> input) {
      BigQueryTable t = input.getKey();
      BigQueryTablePartition p = input.getValue().getKey();
      String filePath = input.getValue().getValue();

      Map<BigQueryTablePartition, Set<String>> tableMap =
          mutableAccumulator.computeIfAbsent(t, k -> new HashMap<>());
      Set<String> fileSet = tableMap.computeIfAbsent(p, k -> new HashSet<>());
      fileSet.add(filePath);

      return mutableAccumulator;
    }

    @Override
    public Map<BigQueryTable, Map<BigQueryTablePartition, Set<String>>> mergeAccumulators(
        Iterable<Map<BigQueryTable, Map<BigQueryTablePartition, Set<String>>>> accumulators) {
      Map<BigQueryTable, Map<BigQueryTablePartition, Set<String>>> result = new HashMap<>();

      accumulators.forEach(
          tableMap -> {
            tableMap
                .entrySet()
                .forEach(
                    e -> {
                      BigQueryTable t = e.getKey();
                      Map<BigQueryTablePartition, Set<String>> partitionMap = e.getValue();
                      result.merge(t, partitionMap, this::mergeTableMaps);
                    });
          });

      return result;
    }

    private Map<BigQueryTablePartition, Set<String>> mergeTableMaps(
        Map<BigQueryTablePartition, Set<String>> m1, Map<BigQueryTablePartition, Set<String>> m2) {
      m2.entrySet()
          .forEach(
              e -> {
                BigQueryTablePartition p = e.getKey();
                Set<String> fileSet = e.getValue();
                m1.merge(p, fileSet, this::mergeFileSets);
              });
      return m1;
    }

    private Set<String> mergeFileSets(Set<String> s1, Set<String> s2) {
      for (String s : s2) {
        if (!s1.add(s)) {
          throw new IllegalStateException("Duplicated file for a partition: " + s);
        }
      }
      return s1;
    }

    @Override
    public Map<BigQueryTable, Map<BigQueryTablePartition, Set<String>>> extractOutput(
        Map<BigQueryTable, Map<BigQueryTablePartition, Set<String>>> accumulator) {
      return accumulator;
    }
  }

  /** Pipeline options supported by {@link UpdateDataplexBigQueryToGcsExportMetadataTransform}. */
  public interface Options extends PipelineOptions {
    @Description("Whether to update Dataplex metadata for the newly created assets. Default: YES.")
    @Default.Boolean(true)
    @Required
    Boolean getUpdateDataplexMetadata();

    void setUpdateDataplexMetadata(Boolean updateDataplexMetadata);
  }
}
