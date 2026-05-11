/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigtable;

import static com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation.MutationType.GARBAGE_COLLECTION;

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.TimestampRange;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.BigtableChangeStreamsToBigtableOptions;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bigtable change stream pipeline to replicate changes to another Cloud Bigtable instance. Pipeline
 * reads from a Bigtable change stream, converts change stream mutations to native protobuf
 * mutations, and writes them directly to Cloud Bigtable.
 */
@Template(
    name = "Bigtable_Change_Streams_to_Bigtable",
    category = TemplateCategory.STREAMING,
    displayName = "Bigtable Change Streams to Bigtable Replicator",
    description =
        "A streaming pipeline that replicates Bigtable change stream mutations to another Bigtable instance",
    optionsClass = BigtableChangeStreamsToBigtableOptions.class,
    flexContainerName = "bigtable-changestreams-to-bigtable",
    contactInformation = "https://cloud.google.com/support",
    streaming = true,
    supportsAtLeastOnce = true)
public class BigtableChangeStreamsToBigtable {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableChangeStreamsToBigtable.class);
  private static final String USE_RUNNER_V2_EXPERIMENT = "use_runner_v2";

  private static void setOptions(BigtableChangeStreamsToBigtableOptions options) {
    options.setStreaming(true);
    options.setEnableStreamingEngine(true);

    // Add use_runner_v2 to the experiments option, since change streams connector is only supported
    // on Dataflow runner v2.
    List<String> experiments = options.getExperiments();
    if (experiments == null) {
      experiments = new ArrayList<>();
    }
    boolean hasUseRunnerV2 = false;
    for (String experiment : experiments) {
      if (experiment.equalsIgnoreCase(USE_RUNNER_V2_EXPERIMENT)) {
        hasUseRunnerV2 = true;
        break;
      }
    }
    if (!hasUseRunnerV2) {
      experiments.add(USE_RUNNER_V2_EXPERIMENT);
    }
    options.setExperiments(experiments);
  }

  /**
   * Creates and runs bigtable to bigtable pipeline.
   *
   * @param pipelineOptions options for reading and writing to bigtable
   * @return PipelineResult
   */
  public static PipelineResult run(BigtableChangeStreamsToBigtableOptions pipelineOptions) {
    setOptions(pipelineOptions);

    Pipeline pipeline = Pipeline.create(pipelineOptions);

    Instant startTime =
        pipelineOptions.getBigtableChangeStreamStartTimestamp().isEmpty()
            ? Instant.now()
            : Instant.parse(pipelineOptions.getBigtableChangeStreamStartTimestamp());

    LOG.info("BigtableChangeStreamsToBigtable pipeline starting from {}", startTime);

    String readProjectId = pipelineOptions.getBigtableReadProjectId();
    if (readProjectId == null || readProjectId.isEmpty()) {
      readProjectId =
          pipelineOptions
              .as(org.apache.beam.sdk.extensions.gcp.options.GcpOptions.class)
              .getProject();
    }

    BigtableIO.ReadChangeStream readChangeStream =
        BigtableIO.readChangeStream()
            .withProjectId(readProjectId)
            .withInstanceId(pipelineOptions.getBigtableReadInstanceId())
            .withTableId(pipelineOptions.getBigtableReadTableId())
            .withAppProfileId(pipelineOptions.getBigtableChangeStreamAppProfile())
            .withStartTime(startTime);

    if (pipelineOptions.getBigtableChangeStreamName() != null
        && !pipelineOptions.getBigtableChangeStreamName().isEmpty()) {
      readChangeStream =
          readChangeStream
              .withChangeStreamName(pipelineOptions.getBigtableChangeStreamName())
              .withExistingPipelineOptions(
                  pipelineOptions.getBigtableChangeStreamResume() != null
                          && pipelineOptions.getBigtableChangeStreamResume()
                      ? BigtableIO.ExistingPipelineOptions.RESUME_OR_FAIL
                      : BigtableIO.ExistingPipelineOptions.FAIL_IF_EXISTS);
    }

    if (pipelineOptions.getBigtableChangeStreamMetadataInstanceId() != null
        && !pipelineOptions.getBigtableChangeStreamMetadataInstanceId().isEmpty()) {
      readChangeStream =
          readChangeStream.withMetadataTableInstanceId(
              pipelineOptions.getBigtableChangeStreamMetadataInstanceId());
    }

    if (pipelineOptions.getBigtableChangeStreamMetadataTableTableId() != null
        && !pipelineOptions.getBigtableChangeStreamMetadataTableTableId().isEmpty()) {
      readChangeStream =
          readChangeStream.withMetadataTableTableId(
              pipelineOptions.getBigtableChangeStreamMetadataTableTableId());
    }

    if (pipelineOptions.getBigtableReadChangeStreamTimeoutMs() != null) {
      readChangeStream =
          readChangeStream.withReadChangeStreamTimeout(
              Duration.millis(pipelineOptions.getBigtableReadChangeStreamTimeoutMs()));
    }

    PCollection<KV<ByteString, ChangeStreamMutation>> changeStream =
        pipeline.apply("Read Change Stream", readChangeStream);

    if (pipelineOptions.getAddRedistribute()) {
      changeStream = changeStream.apply("Redistribute Change Stream", Redistribute.byKey());
    }

    changeStream
        .apply(
            "Convert CDC mutation to Bigtable protobuf mutation",
            ParDo.of(
                new ConvertChangeStreamToNativeMutationFn(
                    pipelineOptions.getBidirectionalReplicationEnabled(),
                    pipelineOptions.getCbtQualifier(),
                    pipelineOptions.getCbtFilterQualifier(),
                    pipelineOptions.getFilterGCMutations())))
        .apply("Write row mutations to Bigtable", createWrite(pipelineOptions));

    return pipeline.run();
  }

  static BigtableIO.Write createWrite(BigtableChangeStreamsToBigtableOptions options) {
    BigtableIO.Write write =
        BigtableIO.write()
            .withInstanceId(options.getBigtableWriteInstanceId())
            .withTableId(options.getBigtableWriteTableId());

    String projectId = options.getBigtableWriteProjectId();
    if (projectId == null || projectId.isEmpty()) {
      projectId =
          options.as(org.apache.beam.sdk.extensions.gcp.options.GcpOptions.class).getProject();
    }
    if (projectId != null && !projectId.isEmpty()) {
      write = write.withProjectId(projectId);
    }
    if (options.getBigtableWriteAppProfile() != null
        && !options.getBigtableWriteAppProfile().isEmpty()) {
      write = write.withAppProfileId(options.getBigtableWriteAppProfile());
    }
    if (options.getBigtableBulkWriteMaxRowKeyCount() != null) {
      write =
          write.withMaxElementsPerBatch(options.getBigtableBulkWriteMaxRowKeyCount().longValue());
    }
    if (options.getBigtableBulkWriteMaxRequestSizeBytes() != null) {
      write =
          write.withMaxBytesPerBatch(options.getBigtableBulkWriteMaxRequestSizeBytes().longValue());
    }
    if (options.getBigtableBulkWriteFlowControl() != null) {
      write = write.withFlowControl(options.getBigtableBulkWriteFlowControl());
    }
    return write;
  }

  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    LOG.info("Starting to replicate change records from Cloud Bigtable change streams to Bigtable");

    BigtableChangeStreamsToBigtableOptions pipelineOptions =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BigtableChangeStreamsToBigtableOptions.class);

    run(pipelineOptions);
  }

  /**
   * Converts Bigtable change stream mutations to native Bigtable v2 protobuf Mutation iterables.
   */
  static class ConvertChangeStreamToNativeMutationFn
      extends DoFn<KV<ByteString, ChangeStreamMutation>, KV<ByteString, Iterable<Mutation>>> {

    private final boolean bidirectionalReplicationEnabled;
    private final String cbtQualifier;
    private final String cbtFilterQualifier;
    private final boolean filterGCMutations;

    ConvertChangeStreamToNativeMutationFn(
        boolean bidirectionalReplicationEnabled,
        String cbtQualifier,
        String cbtFilterQualifier,
        boolean filterGCMutations) {
      this.bidirectionalReplicationEnabled = bidirectionalReplicationEnabled;
      this.cbtQualifier = cbtQualifier;
      this.cbtFilterQualifier = cbtFilterQualifier;
      this.filterGCMutations = filterGCMutations;
    }

    @ProcessElement
    public void processElement(
        @Element KV<ByteString, ChangeStreamMutation> element,
        OutputReceiver<KV<ByteString, Iterable<Mutation>>> receiver)
        throws Exception {

      ChangeStreamMutation mutation = element.getValue();

      // Skip element if filter GC flag on and the mutation was of GC type.
      if (filterGCMutations && mutation.getType().equals(GARBAGE_COLLECTION)) {
        Metrics.counter(ConvertChangeStreamToNativeMutationFn.class, "gc_mutations_filtered").inc();
        return;
      }

      // Skip element if it was replicated.
      if (bidirectionalReplicationEnabled && isReplicated(mutation, cbtFilterQualifier)) {
        return;
      }

      List<Mutation> protoMutations = new ArrayList<>();
      String lastFamily = null;

      for (Entry entry : mutation.getEntries()) {
        if (entry instanceof com.google.cloud.bigtable.data.v2.models.SetCell) {
          com.google.cloud.bigtable.data.v2.models.SetCell setCell =
              (com.google.cloud.bigtable.data.v2.models.SetCell) entry;
          lastFamily = setCell.getFamilyName();

          protoMutations.add(
              Mutation.newBuilder()
                  .setSetCell(
                      Mutation.SetCell.newBuilder()
                          .setFamilyName(setCell.getFamilyName())
                          .setColumnQualifier(setCell.getQualifier())
                          .setTimestampMicros(setCell.getTimestamp())
                          .setValue(setCell.getValue())
                          .build())
                  .build());
        } else if (entry instanceof com.google.cloud.bigtable.data.v2.models.DeleteCells) {
          com.google.cloud.bigtable.data.v2.models.DeleteCells deleteCells =
              (com.google.cloud.bigtable.data.v2.models.DeleteCells) entry;
          lastFamily = deleteCells.getFamilyName();

          Mutation.DeleteFromColumn.Builder delCol =
              Mutation.DeleteFromColumn.newBuilder()
                  .setFamilyName(deleteCells.getFamilyName())
                  .setColumnQualifier(deleteCells.getQualifier());

          long start = deleteCells.getTimestampRange().getStart();
          long end = deleteCells.getTimestampRange().getEnd();

          if (start > 0 || end > 0) {
            delCol.setTimeRange(
                TimestampRange.newBuilder()
                    .setStartTimestampMicros(start)
                    .setEndTimestampMicros(end > 0 ? end : Long.MAX_VALUE)
                    .build());
          }

          protoMutations.add(Mutation.newBuilder().setDeleteFromColumn(delCol).build());
        } else if (entry instanceof com.google.cloud.bigtable.data.v2.models.DeleteFamily) {
          com.google.cloud.bigtable.data.v2.models.DeleteFamily deleteFamily =
              (com.google.cloud.bigtable.data.v2.models.DeleteFamily) entry;
          lastFamily = deleteFamily.getFamilyName();

          protoMutations.add(
              Mutation.newBuilder()
                  .setDeleteFromFamily(
                      Mutation.DeleteFromFamily.newBuilder()
                          .setFamilyName(deleteFamily.getFamilyName())
                          .build())
                  .build());
        } else {
          LOG.warn("Unsupported entry type: {}", entry.getClass().getName());
        }
      }

      if (protoMutations.isEmpty()) {
        return;
      }

      // Append origin information to mutations.
      if (bidirectionalReplicationEnabled && lastFamily != null) {
        protoMutations.add(
            Mutation.newBuilder()
                .setDeleteFromColumn(
                    Mutation.DeleteFromColumn.newBuilder()
                        .setFamilyName(lastFamily)
                        .setColumnQualifier(ByteString.copyFromUtf8(cbtQualifier))
                        .setTimeRange(
                            TimestampRange.newBuilder()
                                .setStartTimestampMicros(0)
                                .setEndTimestampMicros(1)
                                .build())
                        .build())
                .build());
      }

      receiver.output(KV.of(element.getKey(), protoMutations));
    }

    private boolean isReplicated(ChangeStreamMutation mutation, String filterQualifier) {
      List<Entry> mutationEntries = mutation.getEntries();

      if (mutationEntries.isEmpty()) {
        return false;
      }
      Entry lastEntry = mutationEntries.get(mutationEntries.size() - 1);

      if (lastEntry instanceof com.google.cloud.bigtable.data.v2.models.DeleteCells) {
        if (((com.google.cloud.bigtable.data.v2.models.DeleteCells) lastEntry)
            .getQualifier()
            .equals(ByteString.copyFromUtf8(filterQualifier))) {
          Metrics.counter(
                  ConvertChangeStreamToNativeMutationFn.class, "replicated_mutations_filtered")
              .inc();
          return true;
        }
      }
      Metrics.counter(ConvertChangeStreamToNativeMutationFn.class, "bigtable_mutations_replicated")
          .inc();
      return false;
    }
  }
}
