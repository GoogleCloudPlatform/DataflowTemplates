/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.google.cloud.teleport.bigtable;

import com.datastax.driver.core.Session;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.io.cassandra.Mapper;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;

/**
 * This Dataflow Template performs a one off copy of one table from Apache Cassandra to Cloud
 * Bigtable. It is designed to require minimal configuration and aims to replicate the table
 * structure in Cassandra as closely as possible in Cloud Bigtable. To run the pipeline go to <link>
 * enter the required configuration and press <run>
 *
 * <p>The minimum required configuration required to run the pipeline is:
 *
 * <ul>
 *   <li><b>cassandraHosts:</b> The hosts of the Cassandra nodes in a comma separated value list.
 *   <li><b>cassandraPort:</b> The tcp port where Cassandra can be reached on the nodes.
 *   <li><b>cassandraKeyspace:</b> The Cassandra keyspace where the table is located.
 *   <li><b>cassandraTable:</b> The Cassandra table to be copied.
 *   <li><b>bigtableProjectId:</b> The Project ID of the Bigtable instance where the Cassandra table
 *       should be copied.
 *   <li><b>bigtableInstanceId:</b> The Bigtable Instance ID where the Cassandra table should be
 *       copied.
 *   <li><b>bigtableTableId:</b> The name of the Bigtable table where the Cassandra table should be
 *       copied.
 * </ul>
 */
final class CassandraToBigtable {

  public interface Options extends PipelineOptions {

    @Description("Cassandra hosts to read from")
    ValueProvider<String> getCassandraHosts();

    @SuppressWarnings("unused")
    void setCassandraHosts(ValueProvider<String> hosts);

    @Description("Cassandra port")
    @Default.Integer(9042)
    ValueProvider<Integer> getCassandraPort();

    @SuppressWarnings("unused")
    void setCassandraPort(ValueProvider<Integer> port);

    @Description("Cassandra keyspace to read from")
    ValueProvider<String> getCassandraKeyspace();

    @SuppressWarnings("unused")
    void setCassandraKeyspace(ValueProvider<String> keyspace);

    @Description("Cassandra table to read from")
    ValueProvider<String> getCassandraTable();

    @SuppressWarnings("unused")
    void setCassandraTable(ValueProvider<String> cassandraTable);

    @Description("Project where the destination Bigtable instance is located")
    ValueProvider<String> getBigtableProjectId();

    @SuppressWarnings("unused")
    void setBigtableProjectId(ValueProvider<String> projectId);

    @Description("Target Bigtable instance id")
    ValueProvider<String> getBigtableInstanceId();

    @SuppressWarnings("unused")
    void setBigtableInstanceId(ValueProvider<String> bigtableInstanceId);

    @Description("Target Bigtable table")
    ValueProvider<String> getBigtableTableId();

    @SuppressWarnings("unused")
    void setBigtableTableId(ValueProvider<String> bigtableTableId);

    @Description("Default Column Family")
    @Default.String("default")
    ValueProvider<String> getDefaultColumnFamily();

    @SuppressWarnings("unused")
    void setDefaultColumnFamily(ValueProvider<String> defaultColumnFamily);

    @Description("RowKeySeparator")
    @Default.String("#")
    ValueProvider<String> getRowKeySeparator();

    @SuppressWarnings("unused")
    void setRowKeySeparator(ValueProvider<String> rowKeySeparator);
  }

  /**
   * Runs a pipeline to copy one Cassandra table to Cloud Bigtable.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    // Split the Cassandra Hosts value provider into a list value provider.
    ValueProvider.NestedValueProvider<List<String>, String> hosts =
        ValueProvider.NestedValueProvider.of(
            options.getCassandraHosts(),
            (SerializableFunction<String, List<String>>) value -> Arrays.asList(value.split(",")));

    Pipeline p = Pipeline.create(options);

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
            .withCoder(SerializableCoder.of(Row.class));

    BigtableIO.Write sink =
        BigtableIO.write()
            .withProjectId(options.getBigtableProjectId())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId());

    p.apply("Read from Cassandra", source)
        .apply(
            "Convert Row",
            ParDo.of(
                new BeamRowToBigtableFn(
                    options.getRowKeySeparator(), options.getDefaultColumnFamily())))
        .apply("Write to Bigtable", sink);
    p.run();
  }
}
