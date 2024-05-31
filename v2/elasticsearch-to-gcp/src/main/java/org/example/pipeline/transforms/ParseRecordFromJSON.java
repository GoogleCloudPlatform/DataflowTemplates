/*
 * Copyright (C) 2024 Google Inc.
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
 */
package org.example.pipeline.transforms;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.saasquatch.jsonschemainferrer.AdditionalPropertiesPolicies;
import com.saasquatch.jsonschemainferrer.JsonSchemaInferrer;
import com.saasquatch.jsonschemainferrer.SpecVersion;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.example.pipeline.Types;
import org.example.pipeline.json.JsonSchema;
import org.example.pipeline.json.JsonSchema.ResolutionConfig;
import org.example.pipeline.json.JsonSchema.MultiArraySchemaResolution;
import org.example.pipeline.json.JsonSchema.CombinedSchemaResolution;
import org.example.pipeline.json.JsonToAvro;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class ParseRecordFromJSON extends PTransform<PCollection<String>, RecordParseResult> {
  private static final Logger LOG = LoggerFactory.getLogger(ParseRecordFromJSON.class);

  public static ParseRecordFromJSON create() {
    return new ParseRecordFromJSON();
  }

  public interface ParseJSONsOptions extends PipelineOptions {
    @Description("The sample json objects size for schema detection.")
    @Default.Integer(1000)
    Integer getSampleSize();

    void setSampleSize(Integer value);

    @Description("The resolution definition for JSON combined schema translation into AVRO.")
    @Default.Enum("FAVOR_ARRAYS")
    CombinedSchemaResolution getCombinedSchemaResolution();

    void setCombinedSchemaResolution(CombinedSchemaResolution value);

    @Description("The resolution definition for JSON combined schema translation into AVRO.")
    @Default.Enum("DO_NOTHING")
    MultiArraySchemaResolution getMultiArraySchemaResolution();

    void setMultiArraySchemaResolution(MultiArraySchemaResolution value);

    static ResolutionConfig schemaResolutionFromOptions(PipelineOptions options) {
      var parseOptions = options.as(ParseJSONsOptions.class);
      return ResolutionConfig.of(
          parseOptions.getCombinedSchemaResolution(), parseOptions.getMultiArraySchemaResolution());
    }
  }

  @Override
  public RecordParseResult expand(PCollection<String> input) {
    var parseOptions = input.getPipeline().getOptions().as(ParseJSONsOptions.class);
    var sampleSize = parseOptions.getSampleSize();

    var detectedJSONSchema =
        input
            .apply("SampleSomeData", Sample.any(sampleSize))
            .apply(
                "CombineToList",
                Combine.<String, Iterable<String>>globally(ToList.create())
                    .withFanout(sampleSize / 1000))
            .apply("ExtractJSONSchema", ParDo.of(ExtractJSONSchemaFromSample.create()));
    var jsonSchemaView = detectedJSONSchema.apply("ToView", View.asSingleton());

    var parseResult =
        input.apply(
            "ParseJSONs",
            ParDo.of(
                    TransformToAvro.create(
                        jsonSchemaView,
                        ParseJSONsOptions.schemaResolutionFromOptions(parseOptions)))
                .withSideInputs(jsonSchemaView)
                .withOutputTags(
                    TransformToAvro.success,
                    TupleTagList.of(
                        List.of(TransformToAvro.failed, TransformToAvro.detectedSchema))));

    return RecordParseResult.in(
        input.getPipeline(),
        TransformToAvro.failed,
        parseResult.get(TransformToAvro.failed).setRowSchema(Types.NON_PARSED_RECORD_SCHEMA),
        TransformToAvro.success,
        parseResult.get(TransformToAvro.success).setCoder(ByteArrayCoder.of()),
        TransformToAvro.detectedSchema,
        parseResult.get(TransformToAvro.detectedSchema));
  }

  private static final ObjectMapper MAPPER = JsonToAvro.defaultObjectMapperSaneKeys();

  static class ExtractJSONSchemaFromSample extends DoFn<Iterable<String>, String> {
    private static final JsonSchemaInferrer SCHEMA_INFERRER =
        JsonSchemaInferrer.newBuilder()
            .setSpecVersion(SpecVersion.DRAFT_06)
            .setAdditionalPropertiesPolicy(AdditionalPropertiesPolicies.notAllowed())
            .build();

    static ExtractJSONSchemaFromSample create() {
      return new ExtractJSONSchemaFromSample();
    }

    @ProcessElement
    public void process(ProcessContext context) {
      var samples =
          StreamSupport.stream(context.element().spliterator(), false)
              // needed to sanitize the keys, by doing this we can inject our key sanitizer
              .map(json -> JsonToAvro.sanitizedJson(MAPPER, json))
              .toList();
      var schema = SCHEMA_INFERRER.inferForSamples(samples).toString();
      LOG.info("Inferred json schema:\n{}", schema);
      context.output(schema);
    }
  }

  static class TransformToAvro extends DoFn<String, byte[]> {
    private static final Counter SUCCESS =
        Metrics.counter(TransformToAvro.class, "successfully-transformed-count");
    private static final Counter FAILED = Metrics.counter(TransformToAvro.class, "failed-count");

    static TupleTag<Row> failed = new TupleTag<>() {};
    static TupleTag<byte[]> success = new TupleTag<>() {};
    static TupleTag<String> detectedSchema = new TupleTag<>() {};

    private final PCollectionView<String> sampledJSONSchema;
    private final ResolutionConfig schemaResolution;
    private transient Schema avroSchema;
    private transient volatile ObjectMapper objectMapper;
    private String lastErrorMessage = "";

    TransformToAvro(PCollectionView<String> sampledJSONSchema, ResolutionConfig schemaResolution) {
      this.sampledJSONSchema = sampledJSONSchema;
      this.schemaResolution = schemaResolution;
    }

    static TransformToAvro create(
        PCollectionView<String> sampledJSONSchema, ResolutionConfig schemaResolution) {
      return new TransformToAvro(sampledJSONSchema, schemaResolution);
    }

    @ProcessElement
    public void process(ProcessContext context) {
      if (avroSchema == null) {
        var jsonSchema = context.sideInput(sampledJSONSchema);
        avroSchema = JsonSchema.avroSchemaFromJsonSchema(jsonSchema, schemaResolution);
        context.output(detectedSchema, avroSchema.toString());
      }
      try {
        context.output(
            success,
            Types.toByteArray(JsonToAvro.jsonToGenericRecord(objectMapper(), context.element())));
        SUCCESS.inc();
      } catch (Exception ex) {
        FAILED.inc();
        var rootCauseMessage = ExceptionUtils.getRootCauseMessage(ex);
        var errorMessage =
            Optional.ofNullable(ex.getMessage() + " " + rootCauseMessage).orElse(rootCauseMessage);
        Optional.ofNullable(errorMessage)
            .filter(errorMsg -> !errorMsg.equals(lastErrorMessage))
            .ifPresent(
                errorMsg -> {
                  LOG.warn("Problems while parsing JSON.\n{}\n", context.element(), ex);
                  lastErrorMessage = errorMsg;
                });
        context.output(failed, Types.notParsedRow(errorMessage, context.element(), DateTime.now()));
      }
    }

    private ObjectMapper objectMapper() {
      if (this.objectMapper == null) {
        synchronized (this) {
          if (this.objectMapper == null) {
            this.objectMapper =
                JsonToAvro.objectMapperWithDeserializer(
                    MAPPER,
                    JsonToAvro.GenericRecordJsonDeserializer.forSchema(avroSchema)
                        .withMultiArrayBehavior(schemaResolution.arraySchema())
                        .withNullBehavior(
                            JsonToAvro.GenericRecordJsonDeserializer.NullBehavior
                                .ACCEPT_MISSING_OR_NULL));
          }
        }
      }

      return this.objectMapper;
    }
  }

  static class ToList<T> extends Combine.CombineFn<T, List<T>, Iterable<T>> {

    static <K> ToList<K> create() {
      return new ToList<>();
    }

    @Override
    public List<T> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<T> addInput(List<T> accumulator, T input) {
      accumulator.add(input);
      return accumulator;
    }

    @Override
    public List<T> mergeAccumulators(Iterable<List<T>> accumulators) {
      Iterator<List<T>> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator();
      }
      List<T> res = iter.next();
      while (iter.hasNext()) {
        res.addAll(iter.next());
      }
      return res;
    }

    @Override
    public Iterable<T> extractOutput(List<T> accumulator) {
      return accumulator;
    }
  }
}
