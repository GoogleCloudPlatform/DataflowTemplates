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

import com.google.cloud.Timestamp;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.templates.transforms.ConvertChangeStream;
import com.google.cloud.teleport.v2.templates.transforms.HbaseRowMutationIO;
import com.google.cloud.teleport.v2.templates.utils.RowMutationsCoder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.RowMutations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bigtable change stream pipeline to replicate changes to Hbase. Pipeline reads from a Bigtable
 * change stream, converts change stream mutations to their nearest Hbase counterparts, and writes
 * the resulting Hbase row mutations to Hbase.
 */
@Template(
    name = "bigtable-cdc-to-hbase",
    category = TemplateCategory.STREAMING,
    displayName = "Bigtable CDC to HBase Replicator",
    description = "A streaming pipeline that replicates Bigtable change stream data to HBase",
    optionsClass = BigtableToHbasePipeline.BigtableToHbasePipelineOptions.class,
    flexContainerName = "bigtable-cdc-to-hbase",
    contactInformation = "https://cloud.google.com/support")
public class BigtableToHbasePipeline {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableToHbasePipeline.class);
  private static final String USE_RUNNER_V2_EXPERIMENT = "use_runner_v2";

  /** Options to run pipeline with. */
  public interface BigtableToHbasePipelineOptions
      extends DataflowPipelineOptions, ExperimentalOptions {
    /** Bigtable change stream configs. */
    @TemplateParameter.Text(
        description = "Bigtable project id",
        helpText = "GCP project id that bigtable is under")
    String getBigtableProjectId();

    void setBigtableProjectId(String bigtableProjectId);

    @TemplateParameter.Text(
        description = "Bigtable instance",
        helpText = "Bigtable instance to stream changes from")
    String getInstanceId();

    void setInstanceId(String instanceId);

    @TemplateParameter.Text(
        description = "Table id",
        helpText = "The name of the Bigtable table whose changes to stream")
    String getTableId();

    void setTableId(String tableId);

    @TemplateParameter.Text(
        description = "App profile id",
        helpText =
            "Bigtable's app profile id needs to have single-cluster routing with single-row transactions allowed")
    @Default.String("default")
    String getAppProfileId();

    void setAppProfileId(String appProfileId);

    /** Hbase specific configs. Mirrors configurations on hbase-site.xml. */
    @TemplateParameter.Text(
        description = "Zookeeper quorum location",
        helpText = "Zookeeper quorum location, corresponds to hbase.zookeeper.quorum")
    String getHbaseZookeeperQuorum();

    void setHbaseZookeeperQuorum(String hbaseZookeeperQuorum);

    @TemplateParameter.Text(
        description = "Hbase root directory",
        helpText = "Hbase root directory, corresponds to hbase.rootdir")
    String getHbaseRootDir();

    void setHbaseRootDir(String hbaseRootDir);

    @TemplateParameter.Text(
        description = "Hbase cluster distribution mode",
        helpText = "Whether hbase is distributed, corresponds to hbase.cluster.distributed")
    @Default.String("true")
    String getHbaseClusterDistributed();

    void setHbaseClusterDistributed(String hbaseClusterDistributed);

    @TemplateParameter.Boolean(
        description = "Two way replication",
        helpText = "Whether two-way replication between hbase and bigtable is enabled")
    @Default.Boolean(true)
    boolean getTwoWayReplicationEnabled();

    void setTwoWayReplicationEnabled(boolean twoWayReplicationEnabled);

    @TemplateParameter.Text(
        description = "Source CBT qualifier",
        helpText = "Two way replication source CBT qualifier")
    @Default.String("SOURCE_CBT")
    String getCbtQualifier();

    void setCbtQualifier(String cbtQualifier);

    @TemplateParameter.Text(
        description = "Source Hbase qualifier",
        helpText = "Two way replication source Hbase qualifier")
    @Default.String("SOURCE_HBASE")
    String getHbaseQualifier();

    void setHbaseQualifier(String hbaseQualifier);

    @TemplateParameter.DateTime(
        optional = true,
        description = "The timestamp to read change streams from",
        helpText =
            "The starting DateTime, inclusive, to use for reading change streams"
                + " (https://tools.ietf.org/html/rfc3339). For example, 2022-05-05T07:59:59Z."
                + " Defaults to the timestamp when the pipeline starts.")
    @Default.String("")
    String getStartTimestamp();

    void setStartTimestamp(String startTimestamp);

    @TemplateParameter.DateTime(
        optional = true,
        description = "The timestamp to read change streams to",
        helpText =
            "The ending DateTime, inclusive, to use for reading change streams"
                + " (https://tools.ietf.org/html/rfc3339). Ex-2022-05-05T07:59:59Z. Defaults to an"
                + " infinite time in the future.")
    @Default.String("")
    String getEndTimestamp();

    void setEndTimestamp(String startTimestamp);
  }

  /**
   * Creates and runs bigtable to hbase pipeline.
   *
   * @param pipelineOptions
   * @param hbaseConf
   * @return PipelineResult
   */
  public static PipelineResult bigtableToHbasePipeline(
      BigtableToHbasePipelineOptions pipelineOptions, Configuration hbaseConf) {

    Pipeline pipeline = Pipeline.create(pipelineOptions);

    // Set coder for RowMutations class.
    // RowMutations is not serializable so we provide a customer serializer for the class.
    pipeline.getCoderRegistry().registerCoderForClass(RowMutations.class, RowMutationsCoder.of());

    // Retrieve and parse the start / end timestamps.
    Timestamp startTimestamp =
        pipelineOptions.getStartTimestamp().isEmpty()
            ? Timestamp.now()
            : Timestamp.parseTimestamp(pipelineOptions.getStartTimestamp());
    Timestamp endTimestamp =
        pipelineOptions.getEndTimestamp().isEmpty()
            ? Timestamp.MAX_VALUE
            : Timestamp.parseTimestamp(pipelineOptions.getEndTimestamp());

    LOG.info("BigtableToHbasePipeline pipeline.", startTimestamp.toString(), "to", endTimestamp.toString());

    pipeline
        .apply(
            "Read Change Stream",
            BigtableIO.readChangeStream()
                .withProjectId(pipelineOptions.getBigtableProjectId())
                .withInstanceId(pipelineOptions.getInstanceId())
                .withTableId(pipelineOptions.getTableId())
                .withAppProfileId(pipelineOptions.getAppProfileId())
                .withStartTime(startTimestamp)
                .withEndTime(endTimestamp))
        .apply(
            "Convert CDC mutation to HBase mutation",
            ConvertChangeStream.convertChangeStreamMutation()
                .withTwoWayReplication(
                    pipelineOptions.getTwoWayReplicationEnabled(),
                    pipelineOptions.getCbtQualifier(),
                    pipelineOptions.getHbaseQualifier()))
        .apply(
            "Write row mutations to HBase",
            HbaseRowMutationIO.writeRowMutations()
                .withConfiguration(hbaseConf)
                .withTableId(pipelineOptions.getTableId()));

    return pipeline.run();
  }

  public static void main(String[] args) throws IOException {
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
    hbaseConf.set("hbase.zookeeper.quorum", pipelineOptions.getHbaseZookeeperQuorum());
    hbaseConf.set("hbase.rootdir", pipelineOptions.getHbaseRootDir());
    hbaseConf.set("hbase.cluster.distributed", pipelineOptions.getHbaseClusterDistributed());

    bigtableToHbasePipeline(pipelineOptions, hbaseConf);
  }
}
