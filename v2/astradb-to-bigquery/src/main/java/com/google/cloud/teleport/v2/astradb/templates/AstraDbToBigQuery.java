/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.astradb.templates;

import com.datastax.oss.driver.api.core.CqlSession;
import com.dtsx.astra.sdk.db.DatabaseClient;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.astradb.options.AstraDbToBigQueryOptions;
import com.google.cloud.teleport.v2.astradb.transforms.AstraDbToBigQueryMappingFn;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import java.util.AbstractMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.CqlSessionHolder;
import org.apache.beam.sdk.io.astra.db.mapping.AstraDbMapper;
import org.apache.beam.sdk.io.astra.db.mapping.BeamRowDbMapperFactoryFn;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link AstraDbToBigQuery} pipeline is a batch pipeline which ingests data from AstraDB and
 * outputs the resulting records to BigQuery.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/astradb-to-bigquery/README_AstraDB_to_BigQuery.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "AstraDB_To_BigQuery",
    category = TemplateCategory.BATCH,
    displayName = "AstraDB to BigQuery",
    description = {
      "The AstraDB to BigQuery template is a batch pipeline that reads records from AstraDB and writes them to BigQuery.",
      "If the destination table doesn't exist in BigQuery, the pipeline creates a table with the following values:\n"
          + "- The `Dataset ID` is inherited from the Cassandra keyspace.\n"
          + "- The `Table ID` is inherited from the Cassandra table.\n",
      "The schema of the destination table is inferred from the source Cassandra table.\n"
          + "- `List` and `Set` will be mapped to BigQuery `REPEATED` fields.\n"
          + "- `Map` will be mapped to BigQuery `RECORD` fields.\n"
          + "- All other types are mapped to BigQuery fields with the corresponding types.\n"
          + "- Cassandra user-defined types (UDTs) and tuple data types are not supported."
    },
    optionsClass = AstraDbToBigQuery.Options.class,
    flexContainerName = "astradb-to-bigquery",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/astradb-to-bigquery",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {"AstraDB account with a token"})
public class AstraDbToBigQuery {

  /** Logger for the class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(AstraDbToBigQuery.class);

  /** If not provided, it is the default token range value. */
  public static final int DEFAULT_TOKEN_RANGE = 18;

  /**
   * Options for the sample
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options
      extends PipelineOptions,
          AstraDbToBigQueryOptions.AstraDbSourceOptions,
          AstraDbToBigQueryOptions.BigQueryWriteOptions {}

  /** Main operations. */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();
    LOGGER.info("Starting pipeline");

    try {

      Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
      LOGGER.debug("Pipeline Arguments (options) validated");

      // --------------------------------
      // AstraDbIO.Read<Row>
      // --------------------------------

      // Credentials are read from secrets manager
      AbstractMap.SimpleImmutableEntry<String, byte[]> astraCredentials =
          parseAstraCredentials(options);
      LOGGER.debug("Astra Credentials parsed");

      // Map Cassandra Table Schema into BigQuery Table Schema
      SerializableFunction<AstraDbIO.Read<?>, TableSchema> bigQuerySchemaFactory =
          new AstraDbToBigQueryMappingFn(options.getAstraKeyspace(), options.getAstraTable());
      LOGGER.debug("Schema Mapper has been initialized");

      // Map Cassandra Rows into (Apache) Beam Rows (DATA)
      SerializableFunction<CqlSession, AstraDbMapper<Row>> beamRowMapperFactory =
          new BeamRowDbMapperFactoryFn(options.getAstraKeyspace(), options.getAstraTable());
      LOGGER.debug("Row Mapper has been initialized");

      // Distribute reads across all available Cassandra nodes
      int minimalTokenRangesCount =
          (options.getMinTokenRangesCount() == null)
              ? DEFAULT_TOKEN_RANGE
              : options.getMinTokenRangesCount();

      // Source: AstraDb
      AstraDbIO.Read<Row> astraSource =
          AstraDbIO.<Row>read()
              .withToken(astraCredentials.getKey())
              .withSecureConnectBundle(astraCredentials.getValue())
              .withKeyspace(options.getAstraKeyspace())
              .withTable(options.getAstraTable())
              .withMinNumberOfSplits(minimalTokenRangesCount)
              .withMapperFactoryFn(beamRowMapperFactory)
              .withCoder(SerializableCoder.of(Row.class))
              .withEntity(Row.class);
      LOGGER.debug("AstraDb Source initialization [OK]");

      // --------------------------------
      //  BigQueryIO.Write<Row>
      // --------------------------------

      TableReference bqTableRef = parseBigQueryDestinationTable(options);
      createBigQueryDestinationTableIfNotExist(options, bqTableRef);
      LOGGER.debug("BigQuery Sink Table has been initialized");

      // Sink: BigQuery
      BigQueryIO.Write<Row> bigQuerySink =
          BigQueryIO.<Row>write()
              .to(bqTableRef)
              // Specialized function reading cassandra source table and mapping to BigQuery Schema
              .withSchema(bigQuerySchemaFactory.apply(astraSource))
              // Provided by google, convert a Beam Row to a BigQuery TableRow
              .withFormatFunction(row -> row != null ? BigQueryUtils.toTableRow(row) : null)
              // Table Will be created if not exist
              .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
              .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND);
      LOGGER.debug("BigQuery Sink initialization [OK]");

      // --------------------------------
      //  Pipeline
      // --------------------------------

      Pipeline astraDbToBigQueryPipeline = Pipeline.create(options);
      astraDbToBigQueryPipeline
          .apply("Read From Astra", astraSource)
          .apply("Write To BigQuery", bigQuerySink);
      astraDbToBigQueryPipeline.run();

    } finally {
      // Cassandra Connection is stateful and needs to be closed
      CqlSessionHolder.cleanup();
    }
  }

  /**
   * Parse Astra Credentials from secrets in secret Manager. - SecretManagerUtils is not used as
   * only applied to String secrets
   *
   * @param options pipeline options
   * @return a pair with the token and the secure bundle
   */
  private static AbstractMap.SimpleImmutableEntry<String, byte[]> parseAstraCredentials(
      Options options) {

    String astraToken = options.getAstraToken();
    if (!astraToken.startsWith("AstraCS")) {
      astraToken = SecretManagerUtils.getSecret(options.getAstraToken());
    }
    LOGGER.info("Astra Token is parsed, value={}", astraToken.substring(0, 10) + "...");
    /*
     * Accessing the devops Api to retrieve the secure bundle.
     */
    DatabaseClient astraDbClient = new DatabaseClient(astraToken, options.getAstraDatabaseId());
    if (!astraDbClient.exist()) {
      throw new RuntimeException(
          "Astra Database does not exist, please check your Astra Token and Database ID");
    }
    byte[] astraSecureBundle = astraDbClient.downloadDefaultSecureConnectBundle();
    if (!StringUtils.isEmpty(options.getAstraDatabaseRegion())) {
      astraSecureBundle =
          astraDbClient.downloadSecureConnectBundle(options.getAstraDatabaseRegion());
    }
    LOGGER.info("Astra Bundle is parsed, length={}", astraSecureBundle.length);
    return new AbstractMap.SimpleImmutableEntry<>(astraToken, astraSecureBundle);
  }

  /**
   * Create the Bog Query table Reference (provided or based on Cassandra table name).
   *
   * @param options pipeline options
   * @return the big query table reference
   */
  private static TableReference parseBigQueryDestinationTable(Options options) {
    /*
     * bigQueryOutputTableSpec argument is the Big Query table specification. This is parameter
     * is optional. If not set, the table specification is built from the cassandra source table
     * attributes: keyspace=dataset name, table=table name.
     */
    String bigQueryOutputTableSpec = options.getOutputTableSpec();
    if (StringUtils.isEmpty(bigQueryOutputTableSpec)) {
      bigQueryOutputTableSpec =
          options.getProject() + ":" + options.getAstraKeyspace() + "." + options.getAstraTable();
    }
    TableReference bigQueryTableReference = BigQueryUtils.toTableReference(bigQueryOutputTableSpec);
    LOGGER.info("Big Query table spec has been set to {}", bigQueryOutputTableSpec);
    return bigQueryTableReference;
  }

  /**
   * Create destination dataset and tables if needed (schema mapped from Cassandra).
   *
   * @param options pipeline options
   * @param bqTableRef big query table reference
   */
  private static void createBigQueryDestinationTableIfNotExist(
      Options options, TableReference bqTableRef) {
    BigQuery bigquery =
        BigQueryOptions.newBuilder().setProjectId(options.getProject()).build().getService();
    if (null
        == bigquery.getDataset(
            DatasetId.of(bqTableRef.getProjectId(), bqTableRef.getDatasetId()))) {
      LOGGER.info(
          "Dataset was not found: creating DataSet {} in region {}",
          bqTableRef.getDatasetId(),
          options.getWorkerRegion());
      bigquery.create(
          DatasetInfo.newBuilder(bqTableRef.getDatasetId())
              .setLocation(options.getWorkerRegion())
              .build());
      LOGGER.debug("Dataset has been created [OK]");
    } else {
      LOGGER.info("Dataset {} already exist", bqTableRef.getDatasetId());
    }
  }
}
