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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions.ReadChangeStreamOptions;
import com.google.cloud.teleport.v2.templates.transforms.ChangeStreamToRowMutations;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RowMutations;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bigtable change stream pipeline to replicate changes to Hbase. Pipeline reads from a Bigtable
 * change stream, converts change stream mutations to their nearest Hbase counterparts, and writes
 * the resulting Hbase row mutations to Hbase.
 *
 * <p>In Bigtable, all writes to a single row on a single cluster will be streamed in order by
 * commitTimestamp. This is only the case for instances with a single cluster, or if all writes to a
 * given row happen on only one cluster in a replicated instance. This order is maintained in the
 * Dataflow connector via key-ordered delivery, as the Dataflow connector emits ChangeStreamMutation
 * records with the row key as the record key.
 */
@Template(
    name = "Bigtable_Change_Streams_to_HBase",
    category = TemplateCategory.STREAMING,
    displayName = "Bigtable Change Streams to HBase Replicator",
    description = "A streaming pipeline that replicates Bigtable change stream mutations to HBase",
    optionsClass = BigtableChangeStreamsToHBase.BigtableToHbasePipelineOptions.class,
    flexContainerName = "bigtable-changestreams-to-hbase",
    contactInformation = "https://cloud.google.com/support",
    streaming = true,
    supportsAtLeastOnce = true,
    preview = true)
public class BigtableChangeStreamsToHBase {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableChangeStreamsToHBase.class);
  private static final String USE_RUNNER_V2_EXPERIMENT = "use_runner_v2";

  /** Options to run pipeline with. */
  // TODO: merge with BigtableCommon options after CDC GA
  public interface BigtableToHbasePipelineOptions
      extends DataflowPipelineOptions, ExperimentalOptions, ReadChangeStreamOptions {

    /** Hbase specific configs. Mirrors configurations on hbase-site.xml. */
    @TemplateParameter.Text(
        description = "Zookeeper quorum host",
        helpText = "Zookeeper quorum host, corresponds to hbase.zookeeper.quorum host")
    String getHbaseZookeeperQuorumHost();

    void setHbaseZookeeperQuorumHost(String hbaseZookeeperQuorumHost);

    @TemplateParameter.Text(
        optional = true,
        description = "Zookeeper quorum port",
        helpText = "Zookeeper quorum port, corresponds to hbase.zookeeper.quorum port")
    @Default.String("2181")
    String getHbaseZookeeperQuorumPort();

    void setHbaseZookeeperQuorumPort(String hbaseZookeeperQuorumPort);

    @TemplateParameter.Text(
        description = "Hbase root directory",
        helpText = "Hbase root directory, corresponds to hbase.rootdir")
    String getHbaseRootDir();

    void setHbaseRootDir(String hbaseRootDir);

    @TemplateParameter.Boolean(
        optional = true,
        description = "Bidirectional replication",
        helpText =
            "Whether bidirectional replication between hbase and bigtable is enabled, adds additional logic to filter out hbase-replicated mutations")
    @Default.Boolean(false)
    boolean getBidirectionalReplicationEnabled();

    void setBidirectionalReplicationEnabled(boolean bidirectionalReplicationEnabled);

    @TemplateParameter.Text(
        optional = true,
        description = "Source CBT qualifier",
        helpText = "Bidirectional replication source CBT qualifier")
    @Default.String("BIDIRECTIONAL_REPL_SOURCE_CBT")
    String getCbtQualifier();

    void setCbtQualifier(String cbtQualifier);

    @TemplateParameter.Text(
        optional = true,
        description = "Source Hbase qualifier",
        helpText = "Bidirectional replication source Hbase qualifier")
    @Default.String("BIDIRECTIONAL_REPL_SOURCE_HBASE")
    String getHbaseQualifier();

    void setHbaseQualifier(String hbaseQualifier);

    @TemplateParameter.Boolean(
        optional = true,
        description = "Dry run",
        helpText = "When dry run is enabled, pipeline will not write to Hbase")
    @Default.Boolean(false)
    boolean getDryRunEnabled();

    void setDryRunEnabled(boolean dryRunEnabled);

    @TemplateParameter.Boolean(
        optional = true,
        description = "Filter GC mutations",
        helpText = "Filters out garbage collection Delete mutations from CBT")
    @Default.Boolean(false)
    boolean getFilterGCMutations();

    void setFilterGCMutations(boolean filterGCMutations);
  }

  /**
   * Creates and runs bigtable to hbase pipeline.
   *
   * @param pipelineOptions
   * @param hbaseConf
   * @return PipelineResult
   */
  public static PipelineResult run(
      BigtableToHbasePipelineOptions pipelineOptions, Configuration hbaseConf) {

    Pipeline pipeline = Pipeline.create(pipelineOptions);

    Instant startTime =
        pipelineOptions.getBigtableChangeStreamStartTimestamp().isEmpty()
            ? Instant.now()
            : Instant.parse(pipelineOptions.getBigtableChangeStreamStartTimestamp());

    LOG.info("BigtableChangeStreamsToHBase pipeline starting from", startTime.toString());

    PCollection<KV<byte[], RowMutations>> convertedMutations =
        pipeline
            .apply(
                "Read Change Stream",
                BigtableIO.readChangeStream()
                    .withProjectId(pipelineOptions.getBigtableReadProjectId())
                    .withInstanceId(pipelineOptions.getBigtableReadInstanceId())
                    .withTableId(pipelineOptions.getBigtableReadTableId())
                    .withAppProfileId(pipelineOptions.getBigtableReadAppProfile())
                    .withStartTime(startTime))
            .apply(
                "Convert CDC mutation to HBase mutation",
                ChangeStreamToRowMutations.convertChangeStream(
                        pipelineOptions.getFilterGCMutations())
                    .withBidirectionalReplication(
                        pipelineOptions.getBidirectionalReplicationEnabled(),
                        pipelineOptions.getCbtQualifier(),
                        pipelineOptions.getHbaseQualifier()));

    // Write to Hbase if dry run mode is not enabled
    if (pipelineOptions.getDryRunEnabled()) {
      LOG.info("Dry run mode enabled, not writing to Hbase.");
    } else {
      convertedMutations.apply(
          "Write row mutations to HBase",
          HBaseIO.writeRowMutations()
              .withConfiguration(hbaseConf)
              .withTableId(pipelineOptions.getBigtableReadTableId()));
    }

    return pipeline.run();
  }

  public static void main(String[] args) {
    // Create pipeline options from args.
    BigtableToHbasePipelineOptions pipelineOptions =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BigtableToHbasePipelineOptions.class);
    // Add use_runner_v2 to the experiments option, since Change Streams connector is only supported
    // on Dataflow runner v2.
    List<String> experiments = pipelineOptions.getExperiments();
    if (experiments == null) {
      experiments = new ArrayList<>();
    }
    if (!experiments.contains(USE_RUNNER_V2_EXPERIMENT)) {
      experiments.add(USE_RUNNER_V2_EXPERIMENT);
    }
    pipelineOptions.setExperiments(experiments);
    // Set pipeline streaming to be true.
    pipelineOptions.setStreaming(true);

    // Create Hbase-specific connection options.
    Configuration hbaseConf = HBaseConfiguration.create();
    hbaseConf.set(
        HConstants.ZOOKEEPER_QUORUM,
        // Join zookeeper host and port
        pipelineOptions.getHbaseZookeeperQuorumHost()
            + ":"
            + pipelineOptions.getHbaseZookeeperQuorumPort());
    hbaseConf.set(HConstants.HBASE_DIR, pipelineOptions.getHbaseRootDir());

    run(pipelineOptions, hbaseConf);
  }
}
