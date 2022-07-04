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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Schema;
import com.google.cloud.teleport.v2.clients.DataplexClient;
import com.google.cloud.teleport.v2.clients.DataplexClientFactory;
import com.google.cloud.teleport.v2.utils.BigQueryToGcsDirectoryNaming;
import com.google.cloud.teleport.v2.utils.DataplexUtils;
import com.google.cloud.teleport.v2.utils.FileFormat.FileFormatOptions;
import com.google.cloud.teleport.v2.values.BigQueryTable;
import com.google.cloud.teleport.v2.values.BigQueryTablePartition;
import com.google.cloud.teleport.v2.values.DataplexCompression;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
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
import org.apache.beam.sdk.transforms.DoFn.Setup;
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
public class DataplexBigQueryToGcsUpdateMetadata
    extends PTransform<
        PCollection<KV<BigQueryTable, KV<BigQueryTablePartition, String>>>, PCollection<Void>> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataplexBigQueryToGcsUpdateMetadata.class);

  private final FileFormatOptions outputFileFormat;
  private final DataplexCompression outputFileCompression;
  private final DataplexClientFactory dataplexClientFactory;
  private final boolean enforceSamePartitionKey;

  public DataplexBigQueryToGcsUpdateMetadata(
      FileFormatOptions outputFileFormat,
      DataplexCompression outputFileCompression,
      DataplexClientFactory dataplexClientFactory,
      boolean enforceSamePartitionKey) {
    this.outputFileFormat = outputFileFormat;
    this.outputFileCompression = outputFileCompression;
    this.dataplexClientFactory = dataplexClientFactory;
    this.enforceSamePartitionKey = enforceSamePartitionKey;
  }

  @Override
  public PCollection<Void> expand(
      PCollection<KV<BigQueryTable, KV<BigQueryTablePartition, String>>> input) {

    return input
        .apply("CombineMetadata", Combine.globally(new AccumulateEntityMapFn()))
        .apply("UpdateDataplex", ParDo.of(new CallDataplexFn()));
  }

  private class CallDataplexFn
      extends DoFn<Map<BigQueryTable, Map<BigQueryTablePartition, Set<String>>>, Void> {

    private transient DataplexClient dataplex;
    private transient BigQueryToGcsDirectoryNaming directoryNaming;

    @Setup
    public void setup() throws IOException {
      dataplex = dataplexClientFactory.createClient();
      directoryNaming = new BigQueryToGcsDirectoryNaming(enforceSamePartitionKey);
    }

    @ProcessElement
    public void processElement(
        @Element Map<BigQueryTable, Map<BigQueryTablePartition, Set<String>>> input,
        PipelineOptions options) {

      if (!options.as(Options.class).getUpdateDataplexMetadata()) {
        LOG.info("Skipping Dataplex Metadata update.");
      } else {
        // Need to only update entities here. All entities should've been pre-created
        // (but possibly with unspecified schema and format).
        input.forEach(this::updateTableMetadata);
      }
    }

    private void updateTableMetadata(
        BigQueryTable table, Map<BigQueryTablePartition, Set<String>> partitionFiles) {

      ImmutableList<GoogleCloudDataplexV1Entity> entities;
      try {
        entities = dataplex.getEntities(Collections.singletonList(table.getDataplexEntityName()));
      } catch (IOException e) {
        throw new IllegalStateException(
            String.format("Error loading entity: %s", table.getDataplexEntityName()), e);
      }

      if (entities.size() != 1) {
        throw new IllegalStateException(
            String.format(
                "Got %d entities when expected 1 for entity name: %s",
                entities.size(), table.getDataplexEntityName()));
      }

      GoogleCloudDataplexV1Entity entity = entities.iterator().next();

      if (entity == null) {
        throw new IllegalStateException(
            String.format("Got null entity for entity name: %s", table.getDataplexEntityName()));
      }
      if (entity.getSchema() == null
          || entity.getSchema().getUserManaged() == null
          || !entity.getSchema().getUserManaged()) {
        throw new IllegalStateException(
            String.format(
                "Entity %s already exists, but the schema is not user-managed. Only user-managed "
                    + "schemas are supported when Dataplex metadata updates are enabled.",
                entity.getName()));
      }

      entity.setFormat(DataplexUtils.storageFormat(outputFileFormat, outputFileCompression));

      try {
        GoogleCloudDataplexV1Schema schema =
            DataplexUtils.toDataplexSchema(table.getSchema(), table.getPartitioningColumn());
        DataplexUtils.applyHiveStyle(schema, table, directoryNaming);
        schema.setUserManaged(true);
        entity.setSchema(schema);
      } catch (Exception e) {
        throw new IllegalStateException(
            String.format("Couldn't generate Dataplex schema for entity: %s", entity.getName()), e);
      }

      LOG.info("Updating metadata for entity: {}", entity.getName());
      try {
        dataplex.updateEntity(entity);
      } catch (IOException e) {
        throw new IllegalStateException(
            String.format("Error updating entity: %s", entity.getName()), e);
      }

      if (table.isPartitioned()) {
        partitionFiles.forEach((k, v) -> updatePartitionMetadata(table, k, v));
      }
    }

    private void updatePartitionMetadata(
        BigQueryTable table, BigQueryTablePartition partition, Set<String> fileNames) {

      if (fileNames.size() > 1) {
        LOG.warn(
            "Partition {} for entity {} has more than 1 file,"
                + " adding only the first one to Dataplex metadata: {}.",
            partition.getPartitionName(),
            table.getDataplexEntityName(),
            fileNames);
      }

      GoogleCloudDataplexV1Partition p =
          new GoogleCloudDataplexV1Partition()
              .setLocation(fileNames.iterator().next())
              .setValues(Collections.singletonList(partition.getPartitionName()));

      GoogleCloudDataplexV1Partition createdPartition;
      try {
        createdPartition = dataplex.createOrUpdatePartition(table.getDataplexEntityName(), p);
        checkNotNull(createdPartition, "Got null in response to create partition.");
      } catch (Exception e) {
        throw new IllegalStateException(
            String.format(
                "Error creating partition for entity %s with values: %s.",
                table.getDataplexEntityName(), p.getValues()),
            e);
      }
      LOG.info(
          "Created partition {} for entity {}.",
          createdPartition.getName(),
          table.getDataplexEntityName());
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
            tableMap.forEach(
                (t, partitionMap) -> result.merge(t, partitionMap, this::mergeTableMaps));
          });

      return result;
    }

    private Map<BigQueryTablePartition, Set<String>> mergeTableMaps(
        Map<BigQueryTablePartition, Set<String>> m1, Map<BigQueryTablePartition, Set<String>> m2) {
      m2.forEach((p, fileSet) -> m1.merge(p, fileSet, this::mergeFileSets));
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

  /** Pipeline options supported by {@link DataplexBigQueryToGcsUpdateMetadata}. */
  public interface Options extends PipelineOptions {
    @Description(
        "Whether to update Dataplex metadata for the newly created entities. Default: false.")
    @Default.Boolean(false)
    @Required
    Boolean getUpdateDataplexMetadata();

    void setUpdateDataplexMetadata(Boolean updateDataplexMetadata);
  }
}
