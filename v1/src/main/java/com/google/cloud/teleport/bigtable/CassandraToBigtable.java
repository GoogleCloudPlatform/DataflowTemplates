/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.bigtable;

import com.datastax.driver.core.Session;
import com.google.cloud.teleport.bigtable.CassandraToBigtable.Options;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.io.cassandra.Mapper;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Dataflow Template performs a one off copy of one table from Apache Cassandra to Cloud
 * Bigtable. It is designed to require minimal configuration and aims to replicate the table
 * structure in Cassandra as closely as possible in Cloud Bigtable.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Cassandra_To_Cloud_Bigtable.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Cassandra_To_Cloud_Bigtable",
    category = TemplateCategory.BATCH,
    displayName = "Cassandra to Cloud Bigtable",
    description = {
      "The Apache Cassandra to Cloud Bigtable template copies a table from Apache Cassandra to Cloud Bigtable. "
          + "This template requires minimal configuration and replicates the table structure in Cassandra as closely as possible in Cloud Bigtable.",
      "The Apache Cassandra to Cloud Bigtable template is useful for the following:\n"
          + "- Migrating Apache Cassandra database when short downtime is acceptable.\n"
          + "- Periodically replicating Cassandra tables to Cloud Bigtable for global serving."
    },
    optionsClass = Options.class,
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/cassandra-to-bigtable",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The target Bigtable table must exist before running the pipeline.",
      "Network connection between Dataflow workers and Apache Cassandra nodes."
    })
final class CassandraToBigtable {

  /** TODO - refactor to extend BigtableCommonOptions.WriteOptions. */
  public interface Options extends PipelineOptions {

    @TemplateParameter.Text(
        order = 1,
        groupName = "Source",
        regexes = {"^[a-zA-Z0-9\\.\\-,]*$"},
        description = "Cassandra Hosts",
        helpText = "The hosts of the Apache Cassandra nodes in a comma-separated list.")
    ValueProvider<String> getCassandraHosts();

    @SuppressWarnings("unused")
    void setCassandraHosts(ValueProvider<String> hosts);

    @TemplateParameter.Integer(
        order = 2,
        groupName = "Source",
        optional = true,
        description = "Cassandra Port",
        helpText =
            "The TCP port to use to reach Apache Cassandra on the nodes. The default value is `9042`.")
    @Default.Integer(9042)
    ValueProvider<Integer> getCassandraPort();

    @SuppressWarnings("unused")
    void setCassandraPort(ValueProvider<Integer> port);

    @TemplateParameter.Text(
        order = 3,
        groupName = "Source",
        regexes = {"^[a-zA-Z0-9][a-zA-Z0-9_]{0,47}$"},
        description = "Cassandra Keyspace",
        helpText = "The Apache Cassandra keyspace where the table is located.")
    ValueProvider<String> getCassandraKeyspace();

    @SuppressWarnings("unused")
    void setCassandraKeyspace(ValueProvider<String> keyspace);

    @TemplateParameter.Text(
        order = 4,
        groupName = "Source",
        regexes = {"^[a-zA-Z][a-zA-Z0-9_]*$"},
        description = "Cassandra Table",
        helpText = "The Apache Cassandra table to copy.")
    ValueProvider<String> getCassandraTable();

    @SuppressWarnings("unused")
    void setCassandraTable(ValueProvider<String> cassandraTable);

    @TemplateParameter.ProjectId(
        order = 5,
        groupName = "Target",
        description = "Bigtable Project ID",
        helpText = "The Google Cloud project ID associated with the Bigtable instance.")
    ValueProvider<String> getBigtableProjectId();

    @SuppressWarnings("unused")
    void setBigtableProjectId(ValueProvider<String> projectId);

    @TemplateParameter.Text(
        order = 6,
        groupName = "Target",
        regexes = {"[a-z][a-z0-9\\-]+[a-z0-9]"},
        description = "Target Bigtable Instance",
        helpText = "The ID of the Bigtable instance that the Apache Cassandra table is copied to.")
    ValueProvider<String> getBigtableInstanceId();

    @SuppressWarnings("unused")
    void setBigtableInstanceId(ValueProvider<String> bigtableInstanceId);

    @TemplateParameter.Text(
        order = 7,
        groupName = "Target",
        regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
        description = "Target Bigtable Table",
        helpText = "The name of the Bigtable table that the Apache Cassandra table is copied to.")
    ValueProvider<String> getBigtableTableId();

    @SuppressWarnings("unused")
    void setBigtableTableId(ValueProvider<String> bigtableTableId);

    @TemplateParameter.Text(
        order = 8,
        groupName = "Target",
        optional = true,
        regexes = {"[-_.a-zA-Z0-9]+"},
        description = "The Default Bigtable Column Family",
        helpText =
            "The name of the column family of the Bigtable table. The default value is `default`.")
    @Default.String("default")
    ValueProvider<String> getDefaultColumnFamily();

    @SuppressWarnings("unused")
    void setDefaultColumnFamily(ValueProvider<String> defaultColumnFamily);

    @TemplateParameter.Text(
        order = 9,
        groupName = "Target",
        optional = true,
        description = "The Row Key Separator",
        helpText = "The separator used to build row-keys. The default value is `#`.")
    @Default.String("#")
    ValueProvider<String> getRowKeySeparator();

    @SuppressWarnings("unused")
    void setRowKeySeparator(ValueProvider<String> rowKeySeparator);

    @TemplateParameter.Boolean(
        order = 10,
        groupName = "Target",
        optional = true,
        description = "If true, large rows will be split into multiple MutateRows requests",
        helpText =
            "The flag for enabling splitting of large rows into multiple MutateRows requests. Note that when a large row is split between multiple API calls, the updates to the row are not atomic. ")
    ValueProvider<Boolean> getSplitLargeRows();

    void setSplitLargeRows(ValueProvider<Boolean> splitLargeRows);

    // TODO(georgecma) - upgrade template to V2 or modify CassandraIO so column schema is
    // automatically processed.
    @TemplateParameter.GcsReadFile(
        order = 11,
        optional = true,
        description =
            "GCS path to Cassandra JSON column schema. If set, the template will use the schema info to copy Cassandra write time to Bigtable cells",
        helpText =
            "GCS path to schema to copy Cassandra writetimes to Bigtable. The command to generate this schema is ```cqlsh -e \"select json * from system_schema.columns where keyspace_name='$CASSANDRA_KEYSPACE' and table_name='$CASSANDRA_TABLE'`\" > column_schema.json```. Set $WRITETIME_CASSANDRA_COLUMN_SCHEMA to a GCS path, e.g. `gs://$BUCKET_NAME/column_schema.json`. Then upload the schema to GCS: `gcloud storage cp column_schema.json $WRITETIME_CASSANDRA_COLUMN_SCHEMA`. Requires Cassandra version 2.2 onwards for JSON support.")
    ValueProvider<String> getWritetimeCassandraColumnSchema();

    void setWritetimeCassandraColumnSchema(ValueProvider<String> writetimeCassandraColumnSchema);

    @TemplateParameter.Boolean(
        order = 12,
        optional = true,
        description =
            "If true, sets Bigtable cell timestamp to 0 if writetime is not present. Else, the Bigtable cell timestamp defaults to pipeline run time (i.e. now).",
        helpText =
            "The flag for setting Bigtable cell timestamp to 0 if Cassandra writetime is not present. The default behavior for when this flag is not set is to set the Bigtable cell timestamp as the template replication time, i.e. now.")
    @Default.Boolean(false)
    ValueProvider<Boolean> getSetZeroTimestamp();

    void setSetZeroTimestamp(ValueProvider<Boolean> setZeroTimestamp);
  }

  private static final Logger LOG = LoggerFactory.getLogger(CassandraToBigtable.class);

  /**
   * Runs a pipeline to copy one Cassandra table to Cloud Bigtable.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    // Split the Cassandra Hosts value provider into a list value provider.
    ValueProvider.NestedValueProvider<List<String>, String> hosts =
        ValueProvider.NestedValueProvider.of(
            options.getCassandraHosts(),
            (SerializableFunction<String, List<String>>) value -> Arrays.asList(value.split(",")));

    Pipeline p = Pipeline.create(PipelineUtils.tweakPipelineOptions(options));

    // Create a factory method to inject the CassandraRowMapperFn to allow custom type mapping.
    SerializableFunction<Session, Mapper> cassandraObjectMapperFactory =
        new CassandraRowMapperFactory(options.getCassandraTable(), options.getCassandraKeyspace());

    CassandraIO.Read<Row> source =
        CassandraIO.<Row>read()
            .withHosts(hosts)
            .withPort(options.getCassandraPort())
            .withKeyspace(options.getCassandraKeyspace())
            .withTable(options.getCassandraTable())
            .withMapperFactoryFn(cassandraObjectMapperFactory)
            .withEntity(Row.class)
            .withCoder(SerializableCoder.of(Row.class))
            .withQuery(
                new CassandraWritetimeQueryProvider(
                    options.getWritetimeCassandraColumnSchema(),
                    options.getCassandraKeyspace(),
                    options.getCassandraTable()));

    BigtableIO.Write sink =
        BigtableIO.write()
            .withProjectId(options.getBigtableProjectId())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId());

    p.apply("Read from Cassandra", source)
        .apply(
            "Convert Row",
            ParDo.of(
                BeamRowToBigtableFn.createWithSplitLargeRows(
                    options.getRowKeySeparator(),
                    options.getDefaultColumnFamily(),
                    options.getSplitLargeRows(),
                    BeamRowToBigtableFn.MAX_MUTATION_PER_REQUEST,
                    options.getWritetimeCassandraColumnSchema(),
                    options.getSetZeroTimestamp())))
        .apply("Write to Bigtable", sink);
    p.run();
  }
}
