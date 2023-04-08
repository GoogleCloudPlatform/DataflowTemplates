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
package com.google.cloud.syndeo.perf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.pubsublite.ReservationPath;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.syndeo.SyndeoTemplate;
import com.google.cloud.syndeo.transforms.bigtable.BigTableWriteSchemaTransformConfiguration;
import com.google.cloud.syndeo.transforms.pubsub.SyndeoPubsubReadSchemaTransformProvider;
import com.google.cloud.syndeo.transforms.pubsub.SyndeoPubsubWriteSchemaTransformProvider;
import com.google.cloud.syndeo.v1.SyndeoV1;
import com.google.cloud.teleport.it.common.ResourceManager;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.bigtable.BigtableResourceManager;
import com.google.cloud.teleport.it.gcp.bigtable.BigtableResourceManagerCluster;
import com.google.cloud.teleport.it.gcp.bigtable.DefaultBigtableResourceManager;
import com.google.cloud.teleport.it.gcp.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.gcp.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.it.gcp.pubsublite.DefaultPubsubliteResourceManager;
import com.google.cloud.teleport.it.gcp.pubsublite.PubsubLiteResourceManager;
import com.google.cloud.teleport.it.gcp.spanner.DefaultSpannerResourceManager;
import com.google.cloud.teleport.it.gcp.spanner.SpannerResourceManager;
import com.google.cloud.teleport.it.kafka.DefaultKafkaResourceManager;
import com.google.cloud.teleport.it.kafka.KafkaResourceManager;
import com.google.pubsub.v1.SubscriptionName;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryDirectReadSchemaTransformProvider;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryStorageWriteApiSchemaTransformProvider;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteReadSchemaTransformProvider;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteWriteSchemaTransformProvider;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteSchemaTransformProvider;
import org.apache.beam.sdk.io.kafka.KafkaReadSchemaTransformConfiguration;
import org.apache.beam.sdk.io.kafka.KafkaWriteSchemaTransformProvider;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AnyToAnySyndeoTestLT {
  private static final String PROJECT = TestProperties.project();
  private static final String REGION = TestProperties.region();
  private static final String BUCKET = TestProperties.artifactBucket();
  private static final String ZONE_SUFFIX = "-c";
  private static final String TEST_ID = "syndeo-any-to-any-" + UUID.randomUUID();

  protected static final Credentials CREDENTIALS = TestProperties.googleCredentials();
  protected static final CredentialsProvider CREDENTIALS_PROVIDER =
      FixedCredentialsProvider.create(CREDENTIALS);

  private static final String SOURCE_URN =
      TestProperties.getProperty("source", null, TestProperties.Type.PROPERTY);
  private static final String SINK_URN =
      TestProperties.getProperty("sink", null, TestProperties.Type.PROPERTY);

  private static final String CONFIGURATION =
      TestProperties.getProperty("configuration", "local", TestProperties.Type.PROPERTY);

  private static final Map<String, SyndeoTestConfiguration> CONFIGURATIONS =
      Map.of(
          "local",
              SyndeoTestConfiguration.create("DirectRunner", 1000L, Duration.standardSeconds(30)),
          "medium",
              SyndeoTestConfiguration.create(
                  "DataflowRunner", 1_000_000L, Duration.standardMinutes(7)),
          "large",
              SyndeoTestConfiguration.create(
                  "DataflowRunner", 100_000_000L, Duration.standardMinutes(70)));
  private static final List<ResourceManager> RESOURCE_MANAGERS = new ArrayList<>();

  public static final Map<String, TransformProvider> SOURCE_PROVIDERS =
      Map.of(
          "beam:schematransform:org.apache.beam:bigquery_storage_read:v1",
          TransformProvider.create(
              () -> DefaultBigQueryResourceManager.builder(TEST_ID, PROJECT).build(),
              (ResourceManager rm) -> {
                assert rm instanceof BigQueryResourceManager;
                BigQueryResourceManager bqrm = (BigQueryResourceManager) rm;
                bqrm.createDataset(REGION);
                return SinkAndSourceConfigs.create(
                    buildJsonConfig(
                        "beam:schematransform:org.apache.beam:bigquery_storage_write:v1",
                        configToMap(
                            BigQueryStorageWriteApiSchemaTransformProvider
                                .BigQueryStorageWriteApiSchemaTransformConfiguration.builder()
                                .setTable(String.format("%s.%s", bqrm.getDatasetId(), TEST_ID))
                                .setAutoSharding(true)
                                .build())),
                    buildJsonConfig(
                        "beam:schematransform:org.apache.beam:bigquery_storage_read:v1",
                        configToMap(
                            BigQueryDirectReadSchemaTransformProvider
                                .BigQueryDirectReadSchemaTransformConfiguration.builder()
                                .setTableSpec(String.format("%s.%s", bqrm.getDatasetId(), TEST_ID))
                                .build())));
              }),
          "beam:schematransform:org.apache.beam:kafka_read:v1",
          TransformProvider.create(
              wrap(() -> DefaultKafkaResourceManager.builder(TEST_ID).build()),
              (ResourceManager rm) -> {
                assert rm instanceof KafkaResourceManager;
                KafkaResourceManager krm = (KafkaResourceManager) rm;
                // TODO(pabloem): Verify correct number of partitions to use.
                String topicName = krm.createTopic(TEST_ID, 1);
                return SinkAndSourceConfigs.create(
                    buildJsonConfig(
                        "beam:schematransform:org.apache.beam:kafka_write:v1",
                        configToMap(
                            KafkaWriteSchemaTransformProvider.KafkaWriteSchemaTransformConfiguration
                                .builder()
                                .setBootstrapServers(krm.getBootstrapServers())
                                .setTopic(topicName)
                                .setFormat("AVRO")
                                .build())),
                    buildJsonConfig(
                        "beam:schematransform:org.apache.beam:kafka_read:v1",
                        configToMap(
                            KafkaReadSchemaTransformConfiguration.builder()
                                .setAutoOffsetResetConfig("earliest")
                                .setConsumerConfigUpdates(
                                    Map.of(
                                        ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                                        "100",
                                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                        "earliest"))
                                .setBootstrapServers(krm.getBootstrapServers())
                                .setTopic(topicName)
                                .setFormat("AVRO")
                                .setSchema(
                                    AvroUtils.toAvroSchema(SyndeoLoadTestUtils.NESTED_TABLE_SCHEMA)
                                        .toString())
                                .build())));
              }),
          "beam:schematransform:org.apache.beam:pubsublite_read:v1",
          TransformProvider.create(
              DefaultPubsubliteResourceManager::new,
              (ResourceManager rm) -> {
                assert rm instanceof PubsubLiteResourceManager;
                PubsubLiteResourceManager psrm = (PubsubLiteResourceManager) rm;
                ReservationPath rpath =
                    psrm.createReservation("resrv-" + TEST_ID, REGION, PROJECT, 100L);
                TopicName tname = psrm.createTopic("topic-" + TEST_ID, rpath);
                String subsName = "sub-" + TEST_ID;
                com.google.cloud.pubsublite.SubscriptionName sname =
                    psrm.createSubscription(rpath, tname, subsName);

                return SinkAndSourceConfigs.create(
                    buildJsonConfig(
                        "beam:schematransform:org.apache.beam:pubsublite_write:v1",
                        configToMap(
                            PubsubLiteWriteSchemaTransformProvider
                                .PubsubLiteWriteSchemaTransformConfiguration.builder()
                                .setFormat("AVRO")
                                .setLocation(rpath.location().value())
                                .setProject(rpath.project().toString())
                                .setTopicName(tname.value())
                                .build())),
                    buildJsonConfig(
                        "beam:schematransform:org.apache.beam:pubsublite_read:v1",
                        configToMap(
                            PubsubLiteReadSchemaTransformProvider
                                .PubsubLiteReadSchemaTransformConfiguration.builder()
                                .setFormat("AVRO")
                                .setSchema(
                                    AvroUtils.toAvroSchema(SyndeoLoadTestUtils.NESTED_TABLE_SCHEMA)
                                        .toString())
                                .setLocation(rpath.location().value())
                                .setProject(rpath.project().toString())
                                .setSubscriptionName(subsName)
                                .build()))); // TODO
              }),
          "syndeo:schematransform:com.google.cloud:pubsub_read:v1",
          TransformProvider.create(
              wrap(
                  () ->
                      DefaultPubsubResourceManager.builder(TEST_ID, PROJECT)
                          .credentialsProvider(CREDENTIALS_PROVIDER)
                          .build()),
              (ResourceManager rm) -> {
                assert rm instanceof PubsubResourceManager;
                PubsubResourceManager psrm = (PubsubResourceManager) rm;
                com.google.pubsub.v1.TopicName tname = psrm.createTopic("topic-" + TEST_ID);
                SubscriptionName sname = psrm.createSubscription(tname, "subscription-" + TEST_ID);

                return SinkAndSourceConfigs.create(
                    buildJsonConfig(
                        "syndeo:schematransform:com.google.cloud:pubsub_write:v1",
                        configToMap(
                            SyndeoPubsubWriteSchemaTransformProvider.SyndeoPubsubWriteConfiguration
                                .create("AVRO", tname.toString()))),
                    buildJsonConfig(
                        "syndeo:schematransform:com.google.cloud:pubsub_read:v1",
                        configToMap(
                            SyndeoPubsubReadSchemaTransformProvider
                                .SyndeoPubsubReadSchemaTransformConfiguration.builder()
                                .setFormat("AVRO")
                                .setSchema(
                                    AvroUtils.toAvroSchema(SyndeoLoadTestUtils.NESTED_TABLE_SCHEMA)
                                        .toString())
                                .setSubscription(sname.toString())
                                .build())));
              }));
  public static final Map<String, TransformProvider> SINK_PROVIDERS =
      Map.of(
          "beam:schematransform:org.apache.beam:bigquery_storage_write:v1",
          TransformProvider.create(
              SOURCE_PROVIDERS
                  .get("beam:schematransform:org.apache.beam:bigquery_storage_read:v1")
                  .getResourceManagerBuilder(),
              (ResourceManager rm) -> {
                assert rm instanceof BigQueryResourceManager;
                BigQueryResourceManager bqrm = (BigQueryResourceManager) rm;
                bqrm.createDataset(REGION);
                return SinkAndSourceConfigs.create(
                    buildJsonConfig(
                        "beam:schematransform:org.apache.beam:bigquery_storage_write:v1",
                        configToMap(
                            BigQueryStorageWriteApiSchemaTransformProvider
                                .BigQueryStorageWriteApiSchemaTransformConfiguration.builder()
                                .setAutoSharding(true)
                                .setTable(
                                    String.format(
                                        "%s.%s.%s", PROJECT, bqrm.getDatasetId(), TEST_ID))
                                .build())),
                    null);
              }),
          "beam:schematransform:org.apache.beam:spanner_write:v1",
          TransformProvider.create(
              () -> DefaultSpannerResourceManager.builder(TEST_ID, PROJECT, REGION).build(),
              (ResourceManager rm) -> {
                assert rm instanceof SpannerResourceManager;
                SpannerResourceManager srm = (SpannerResourceManager) rm;
                // TODO(pabloem): Verify that Syndeo can automatically create the table.
                return SinkAndSourceConfigs.create(
                    buildJsonConfig(
                        "beam:schematransform:org.apache.beam:spanner_write:v1",
                        configToMap(
                            SpannerWriteSchemaTransformProvider
                                .SpannerWriteSchemaTransformConfiguration.builder()
                                .setDatabaseId(srm.getDatabaseId())
                                .setInstanceId(srm.getInstanceId())
                                .setTableId(TEST_ID)
                                .build())),
                    null);
              }),
          "beam:schematransform:org.apache.beam:kafka_write:v1",
          TransformProvider.create(
              wrap(() -> DefaultKafkaResourceManager.builder(TEST_ID).build()),
              (ResourceManager rm) -> {
                assert rm instanceof KafkaResourceManager;
                KafkaResourceManager krm = (KafkaResourceManager) rm;
                // TODO(pabloem): Make sure it's not created twice

                return SinkAndSourceConfigs.create(
                    buildJsonConfig(
                        "beam:schematransform:org.apache.beam:kafka_write:v1",
                        configToMap(
                            KafkaWriteSchemaTransformProvider.KafkaWriteSchemaTransformConfiguration
                                .builder()
                                .setBootstrapServers(krm.getBootstrapServers())
                                .setTopic(TEST_ID)
                                .setFormat("AVRO")
                                .build())),
                    null);
              }),
          //          "beam:schematransform:org.apache.beam:file_write:v1",
          //          TransformProvider.create(null, (ResourceManager rm) -> {
          //            return SinkAndSourceConfigs.create(buildJsonConfig(
          //                    "beam:schematransform:org.apache.beam:file_write:v1",
          //                    configToMap(FileWriteSchemaTransformConfiguration.builder()
          //                            .setFormat("AVRO")
          //                            .setFilenamePrefix("gs://" + Paths.get(BUCKET, TEST_ID))
          //                            .setNumShards(5)  // TODO(pabloem): Verify the correct
          // number of shards.
          //                            .build())), null);
          //          }),
          "beam:schematransform:org.apache.beam:pubsublite_write:v1",
          TransformProvider.create(
              SOURCE_PROVIDERS
                  .get("beam:schematransform:org.apache.beam:pubsublite_read:v1")
                  .getResourceManagerBuilder(),
              (ResourceManager rm) -> {
                assert rm instanceof PubsubLiteResourceManager;
                PubsubLiteResourceManager psrm = (PubsubLiteResourceManager) rm;
                ReservationPath rpath =
                    psrm.createReservation("resrv-wr-" + TEST_ID, REGION, PROJECT, 100L);
                TopicName tname = psrm.createTopic("topic-wr-" + TEST_ID, rpath);
                psrm.createSubscription(rpath, tname, "subscr-wr-" + TEST_ID);
                return SinkAndSourceConfigs.create(
                    buildJsonConfig(
                        "beam:schematransform:org.apache.beam:pubsublite_write:v1",
                        configToMap(
                            PubsubLiteWriteSchemaTransformProvider
                                .PubsubLiteWriteSchemaTransformConfiguration.builder()
                                .setFormat("AVRO")
                                .setLocation(rpath.location().value())
                                .setProject(rpath.project().toString())
                                .setTopicName(tname.value())
                                .build())),
                    null);
              }),
          "syndeo:schematransform:com.google.cloud:bigtable_write:v1",
          TransformProvider.create(
              wrap(() -> DefaultBigtableResourceManager.builder(TEST_ID, PROJECT).build()),
              (ResourceManager rm) -> {
                assert rm instanceof BigtableResourceManager;
                BigtableResourceManager btrm = (BigtableResourceManager) rm;
                btrm.createInstance(
                    Collections.singletonList(
                        BigtableResourceManagerCluster.create(
                            "cluster-" + TEST_ID.substring(0, 15),
                            REGION + ZONE_SUFFIX,
                            10,
                            StorageType.SSD)));
                String tableName = ("table-" + TEST_ID).substring(0, 29);
                btrm.createTable(
                    tableName, SyndeoLoadTestUtils.NESTED_TABLE_SCHEMA.getFieldNames()); // TODO
                return SinkAndSourceConfigs.create(
                    buildJsonConfig(
                        "syndeo:schematransform:com.google.cloud:bigtable_write:v1",
                        configToMap(
                            BigTableWriteSchemaTransformConfiguration.builder()
                                .setInstanceId(btrm.getInstanceId())
                                .setTableId(tableName)
                                .setProjectId(PROJECT)
                                .setKeyColumns(List.of("commit")) // TODO(pabloem): Figure this out
                                .build())),
                    null);
              }));

  static Map<String, Object> dataGeneratorConfiguration() {
    return Map.of(
        "urn",
        "syndeo_test:schematransform:com.google.cloud:generate_data:v1",
        "configurationParameters",
        Map.of(
            "numRows",
            CONFIGURATIONS.get(CONFIGURATION).getNumRows(),
            "runtimeSeconds",
            CONFIGURATIONS.get(CONFIGURATION).getRuntime().getStandardSeconds()));
  }

  @BeforeClass
  public void setUpSourceAndSink() {}

  @Test
  public void testSelectedSinkAndSelectedSource() throws JsonProcessingException {
    String sourceUrn =
        SOURCE_URN != null
            ? SOURCE_URN
            : new ArrayList<>(SOURCE_PROVIDERS.keySet())
                .get(new Random().nextInt(SOURCE_PROVIDERS.size()));
    String sinkUrn =
        SINK_URN != null
            ? SINK_URN
            : new ArrayList<>(SINK_PROVIDERS.keySet())
                .get(new Random().nextInt(SOURCE_PROVIDERS.size()));
    runTestWithSinkAndSource(sinkUrn, sourceUrn);
  }

  public void runTestWithSinkAndSource(String sinkUrn, String sourceUrn)
      throws JsonProcessingException {
    TransformProvider sourceProvider = SOURCE_PROVIDERS.get(sourceUrn);
    ResourceManager sourceRm = sourceProvider.getResourceManagerBuilder().get();
    RESOURCE_MANAGERS.add(sourceRm);
    SinkAndSourceConfigs sourceConfigs =
        sourceProvider.getConfigurationParametersBuilder().apply(sourceRm);

    TransformProvider sinkProvider = SINK_PROVIDERS.get(sinkUrn);
    ResourceManager sinkRm = sinkProvider.getResourceManagerBuilder().get();
    RESOURCE_MANAGERS.add(sinkRm);
    SinkAndSourceConfigs sinkConfigs =
        sinkProvider.getConfigurationParametersBuilder().apply(sinkRm);

    String generatorPipelinePayload =
        buildPipelinePayload(dataGeneratorConfiguration(), sourceConfigs.getSinkConfig());
    SyndeoV1.PipelineDescription generatorDescription =
        SyndeoTemplate.buildFromJsonPayload(generatorPipelinePayload);

    String syndeoPipelinePayload =
        buildPipelinePayload(sourceConfigs.getSourceConfig(), sinkConfigs.getSinkConfig());
    SyndeoV1.PipelineDescription syndeoPipeDescription =
        SyndeoTemplate.buildFromJsonPayload(syndeoPipelinePayload);
  }

  @AfterClass
  public void cleanup() {
    ResourceManagerUtils.cleanResources(RESOURCE_MANAGERS.toArray(new ResourceManager[0]));
    RESOURCE_MANAGERS.clear();
  }

  @AutoValue
  abstract static class SyndeoTestConfiguration {
    public abstract String getRunner();

    public abstract Long getNumRows();

    public abstract Duration getRuntime();

    public static SyndeoTestConfiguration create(String runner, Long numRows, Duration runtime) {
      return new AutoValue_AnyToAnySyndeoTestLT_SyndeoTestConfiguration(runner, numRows, runtime);
    }
  }

  @AutoValue
  public abstract static class TransformProvider {
    public abstract Supplier<ResourceManager> getResourceManagerBuilder();

    public abstract SerializableFunction<ResourceManager, SinkAndSourceConfigs>
        getConfigurationParametersBuilder();

    public static TransformProvider create(
        Supplier<ResourceManager> resourceManagerBuilder,
        SerializableFunction<ResourceManager, SinkAndSourceConfigs>
            configurationParametersBuilder) {
      return new AutoValue_AnyToAnySyndeoTestLT_TransformProvider(
          resourceManagerBuilder, configurationParametersBuilder);
    }
  }

  public static String buildPipelinePayload(Map<String, Object> source, Map<String, Object> sink) {
    return SyndeoLoadTestUtils.mapToJsonPayload(Map.of("source", source, "sink", sink));
  }

  @AutoValue
  public abstract static class SinkAndSourceConfigs {
    public abstract Map<String, Object> getSinkConfig();

    public abstract @Nullable Map<String, Object> getSourceConfig();

    public static SinkAndSourceConfigs create(
        Map<String, Object> sinkConfig, Map<String, Object> sourceConfig) {
      return new AutoValue_AnyToAnySyndeoTestLT_SinkAndSourceConfigs(sinkConfig, sourceConfig);
    }
  }

  static Map<String, Object> buildJsonConfig(
      String urn, Map<String, Object> configurationParameters) {
    return Map.of("urn", urn, "configurationParameters", configurationParameters);
  }

  static <ConfigT> Map<String, Object> configToMap(ConfigT config) {
    try {
      Row rowConfig =
          SchemaRegistry.createDefault()
              .getToRowFunction((Class<ConfigT>) config.getClass())
              .apply(config);
      return rowConfig.getSchema().getFields().stream()
          .filter(f -> rowConfig.getValue(f.getName()) != null)
          .map(f -> Map.entry(f.getName(), rowConfig.getValue(f.getName())))
          .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
  }

  interface FunctionThatThrows<T> {
    T apply() throws Exception;
  }

  static <T> Supplier<T> wrap(FunctionThatThrows<T> f) {
    return () -> {
      try {
        return f.apply();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }
}
