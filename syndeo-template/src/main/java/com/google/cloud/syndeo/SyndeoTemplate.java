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
package com.google.cloud.syndeo;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.syndeo.common.ProviderUtil;
import com.google.cloud.syndeo.common.ProviderUtil.TransformSpec;
import com.google.cloud.syndeo.transforms.SyndeoStatsSchemaTransformProvider;
import com.google.cloud.syndeo.v1.SyndeoV1.ConfiguredSchemaTransform;
import com.google.cloud.syndeo.v1.SyndeoV1.PipelineDescription;
import com.google.common.io.CharStreams;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyndeoTemplate {

  private static final Logger LOG = LoggerFactory.getLogger(SyndeoTemplate.class);

  public interface Options extends PipelineOptions, DataflowPipelineOptions {
    @Description("Pipeline Options.")
    @Nullable
    String getPipelineSpec();

    void setPipelineSpec(String gcsSpec);

    @Description("JSON Spec Payload. Consistent with JSON schema in sampleschema.json")
    @Nullable
    String getJsonSpecPayload();

    void setJsonSpecPayload(String jsonSpecPayload);
  }

  public static final Map<String, Set<String>> SUPPORTED_SCALAR_TRANSFORM_URNS =
      Map.of(
          "syndeo:schematransform:com.google.cloud:sql_transform:v1",
          Set.of(),
          "syndeo:schematransform:com.google.cloud:sql_scalar_transform:v1",
          Set.of());

  public static final Map<String, Set<String>> SUPPORTED_URNS =
      new HashMap<>(
          Map.of(
              // New names:
              "beam:schematransform:org.apache.beam:bigquery_storage_write:v1",
              Set.of(),
              "beam:schematransform:org.apache.beam:bigquery_storage_read:v1",
              Set.of(),
              "beam:schematransform:org.apache.beam:spanner_write:v1",
              Set.of(),
              "beam:schematransform:org.apache.beam:kafka_read:v1",
              Set.of(),
              "beam:schematransform:org.apache.beam:kafka_write:v1",
              Set.of(),
              "beam:schematransform:org.apache.beam:file_write:v1",
              Set.of(),
              "beam:schematransform:org.apache.beam:pubsublite_write:v1",
              Set.of(),
              "beam:schematransform:org.apache.beam:pubsublite_read:v1",
              Set.of()));

  static {
    SUPPORTED_URNS.putAll(
        Map.of(
            "syndeo:schematransform:com.google.cloud:pubsub_read:v1",
            Set.of(),
            "syndeo:schematransform:com.google.cloud:pubsub_write:v1",
            Set.of(),
            "syndeo:schematransform:com.google.cloud:pubsub_dlq_write:v1",
            Set.of(),
            "syndeo:schematransform:com.google.cloud:bigtable_write:v1",
            Set.of("instanceId", "tableId", "keyColumns", "projectId", "appProfileId")));
    SUPPORTED_URNS.putAll(SUPPORTED_SCALAR_TRANSFORM_URNS);
  }

  public static void main(String[] args) throws Exception {
    run(args);
  }

  public static PipelineResult run(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);
    validateOptions(options);

    FileSystems.setDefaultPipelineOptions(options);
    PipelineDescription pipeline;
    if (options.getPipelineSpec() != null) {
      pipeline = readFromFile(options.getPipelineSpec());
    } else {
      pipeline = buildFromJsonPayload(options.getJsonSpecPayload());
    }
    return run(options, pipeline);
  }

  public static PipelineResult run(PipelineOptions options, PipelineDescription description) {
    Pipeline p = Pipeline.create(options);
    // Run pipeline from configuration.
    buildPipeline(p, description);
    return p.run();
  }

  public static void buildPipeline(Pipeline p, PipelineDescription description) {
    // Read proto as configuration.
    List<TransformSpec> specs = new ArrayList<>();
    for (ConfiguredSchemaTransform inst : description.getTransformsList()) {
      specs.add(new TransformSpec(inst));
    }
    ProviderUtil.applyConfigs(specs, PCollectionRowTuple.empty(p));
  }

  public static ConfiguredSchemaTransform buildSyndeoStats(String parent) {
    SchemaTransformProvider transformProvider = new SyndeoStatsSchemaTransformProvider();
    return new ProviderUtil.TransformSpec(
            transformProvider.identifier(), Collections.singletonList(parent))
        .toProto();
  }

  public static ConfiguredSchemaTransform buildFromJsonConfig(JsonNode transformConfig) {
    JsonNode params = transformConfig.get("configurationParameters");
    SchemaTransformProvider transformProvider =
        ProviderUtil.getProvider(transformConfig.get("urn").asText());
    if (transformProvider == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unable to load a transform provider for urn [%s]. Available providers are: %s",
              transformConfig.get("urn").asText(), ProviderUtil.PROVIDERS.keySet()));
    }
    LOG.info(
        "Transform provider({}) is: {} | in {}",
        transformConfig.get("urn").asText(),
        transformProvider,
        transformProvider.getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
    List<Object> configurationParameters =
        transformProvider.configurationSchema().getFields().stream()
            .map(field -> field.getName())
            .map(fieldName -> params.has(fieldName) ? params.get(fieldName) : null)
            .map(
                fieldNode ->
                    fieldNode == null
                        ? null
                        : fieldNode.isBoolean()
                            ? fieldNode.asBoolean()
                            : fieldNode.isFloatingPointNumber()
                                ? fieldNode.asDouble()
                                : fieldNode.isNumber()
                                    ? fieldNode.asLong()
                                    : !fieldNode.isContainerNode()
                                        ? fieldNode.asText()
                                        : fieldNode.isArray()
                                            ? new ObjectMapper().convertValue(fieldNode, List.class)
                                            : convertJsonNodeToRow(fieldNode))
            .collect(Collectors.toList());
    // return new ProviderUtil.TransformSpec(
    //         transformConfig.get("urn").asText(), configurationParameters)
    //     .toProto();
    ConfiguredSchemaTransform t = new ProviderUtil.TransformSpec(
            transformConfig.get("urn").asText(), configurationParameters)
        .toProto();

    LOG.warn("configured schema transfrm " + t.getConfigurationValues().toString());
    // LOG.warn("fiel " + t.getConfigurationValues().)
    return t;
  }

  private static Row convertJsonNodeToRow(JsonNode node) {
    try {
      Map<String, Object> map = new ObjectMapper().convertValue(node, Map.class);
      Schema.Builder schemaBuilder = Schema.builder();
      List<Object> configList = new ArrayList<Object>();

      for (Map.Entry<String, Object> kv : map.entrySet()) {
        String key = kv.getKey();
        Object value = kv.getValue();

        Field field =
            value.getClass() == Integer.class
                ? Field.of(key, FieldType.INT32)
                : value.getClass() == String.class
                    ? Field.of(key, FieldType.STRING)
                    : value.getClass() == Boolean.class ? Field.of(key, FieldType.BOOLEAN) : null;

        if (field == null) {
          throw new IllegalArgumentException(
              "Cannot parse the schema field for " + value.getClass());
        }

        schemaBuilder.addField(field);
        configList.add(value);
      }
      Schema schema = schemaBuilder.build();

      Row row = Row.withSchema(schema).addValues(configList).build();
      LOG.warn("row after converting from json" + row.toString(true));
      return row;
      // return Row.withSchema(schema).addValues(configList).build();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Cannot parse nest JSON input into Beam Row. Input given is: " + node.toString(), e);
    }
  }

  public static PipelineDescription buildFromJsonPayload(String jsonPayload)
      throws JsonProcessingException {
    ObjectMapper om = new ObjectMapper();
    JsonNode config = om.readTree(jsonPayload);
    LOG.info("Initializing with JSON Config: {}", config);
    List<ConfiguredSchemaTransform> transforms = new ArrayList<>();

    // Adding the source transform
    transforms.add(buildFromJsonConfig(config.get("source")));
    transforms.add(buildSyndeoStats(config.get("source").get("urn").asText()));

    if (config.has("transform")) {
      if (!SUPPORTED_SCALAR_TRANSFORM_URNS.containsKey(
          config.get("transform").get("urn").asText())) {
        throw new IllegalArgumentException(
            String.format(
                "Intermediate transform with URN %s not supported. Only %s are supported.",
                config.get("transform").get("urn"),
                SUPPORTED_SCALAR_TRANSFORM_URNS.keySet().stream()
                    .sorted()
                    .collect(Collectors.toList())));
      }
      // Adding the intermediate transform
      transforms.add(buildFromJsonConfig(config.get("transform")));
      transforms.add(buildSyndeoStats(config.get("transform").get("urn").asText()));
    }
    // Add the sink transform
    transforms.add(buildFromJsonConfig(config.get("sink")));

    // Adding the dlq transform, if present
    if (config.get("dlq") != null) {
      transforms.add(buildFromJsonConfig(config.get("dlq")));
    }

    return PipelineDescription.newBuilder().addAllTransforms(transforms).build();
  }

  public static PipelineDescription readFromFile(String filename) {
    try {
      MatchResult result = FileSystems.match(filename);
      checkArgument(
          result.status() == Status.OK && !result.metadata().isEmpty(),
          "Failed to match any files with the pattern: " + filename);
      checkArgument(result.metadata().size() == 1, "Only expected one match!");
      ResourceId id = result.metadata().stream().findFirst().get().resourceId();
      Reader reader = Channels.newReader(FileSystems.open(id), StandardCharsets.ISO_8859_1.name());
      return PipelineDescription.parseFrom(
          ByteString.copyFrom(CharStreams.toString(reader), StandardCharsets.ISO_8859_1));
    } catch (IOException e) {
      throw new RuntimeException("Issue reading file.", e);
    }
  }

  private static void validateOptions(Options inputOptions) {
    if (inputOptions.getPipelineSpec() != null && inputOptions.getJsonSpecPayload() != null) {
      throw new IllegalArgumentException(
          "Template received both --pipelineSpec and --jsonSpecPayload parameters. "
              + "Only one of these parameters should be specified.");
    } else if (inputOptions.getPipelineSpec() == null
        && inputOptions.getJsonSpecPayload() == null) {
      throw new IllegalArgumentException(
          "Template received neither of --pipelineSpec or --jsonSpecPayload parameters. "
              + "One of these parameters must be specified.");
    }
    List<String> experiments =
        inputOptions.getExperiments() == null
            ? new ArrayList<>()
            : new ArrayList<>(inputOptions.getExperiments());
    experiments.addAll(
        List.of(
            "enable_streaming_engine",
            "enable_streaming_auto_sharding=true",
            "streaming_auto_sharding_algorithm=FIXED_THROUGHPUT"));
    inputOptions.as(BigQueryOptions.class).setNumStorageWriteApiStreams(50);
    inputOptions.setExperiments(experiments);
  }
}
