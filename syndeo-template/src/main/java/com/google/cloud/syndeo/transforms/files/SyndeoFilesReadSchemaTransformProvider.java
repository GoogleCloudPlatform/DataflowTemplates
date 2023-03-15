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
package com.google.cloud.syndeo.transforms.files;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import com.google.cloud.syndeo.transforms.TypedSchemaTransformProvider;
import java.nio.file.Path;
import java.util.List;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.Deduplicate;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;

/**
 * Provides a SchemaTransform for reading files from Google Cloud Storage and transforming them to
 * {@link Row}s.
 *
 * <p>Requires a configuration of type {@link
 * SyndeoFilesReadSchemaTransformProvider.SyndeoFilesReadSchemaTransformConfiguration}.
 *
 * <p>The input is expected to be an empty {@link PCollectionRowTuple}. The output is a {@link
 * PCollectionRowTuple} with two collections: "output" and "errors".
 *
 * <p>The "output" collection contains the rows read from the input files, while the "errors"
 * collection contains any errors that occurred during matching of Pubsub Notifications or
 * deserialization of the files.
 *
 * <p><b>Note that two different sources of errors are joined as the error output of this
 * transform.</b>
 */
public class SyndeoFilesReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        SyndeoFilesReadSchemaTransformProvider.SyndeoFilesReadSchemaTransformConfiguration> {

  public static final String SUPPORTED_FORMATS_STR = "AVRO,JSON";

  static final TupleTag<Row> ERROR_TAG = new TupleTag<>("errors");
  static final TupleTag<KV<String, Long>> FILE_GENERATION_OUTPUT_TAG =
      new TupleTag<>("filegen_output");
  static final TupleTag<Row> JSON_ROW_OUTPUT_TAG = new TupleTag<>("output");

  public static final Schema ERROR_SCHEMA =
      Schema.builder().addNullableStringField("row").addStringField("error").build();

  @Override
  public Class<SyndeoFilesReadSchemaTransformConfiguration> configurationClass() {
    return SyndeoFilesReadSchemaTransformConfiguration.class;
  }

  @Override
  public SchemaTransform from(SyndeoFilesReadSchemaTransformConfiguration configuration) {
    if (configuration.getPubsubSubscription() == null) {
      throw new IllegalArgumentException(
          "A pubsub subscription must be provided for notifications to be consumed. "
              + "Automatically setting up notifications from a GCS prefix is not yet supported.");
    }

    if (configuration.getPerformBackfill() != null && configuration.getPerformBackfill()) {
      throw new IllegalArgumentException("Backfills of files in GCS are not currently supported");
      // TODO(pabloem): Enable the check below.
      // if (configuration.getGcsPrefix() == null) {
      //   throw new IllegalArgumentException("To perform a backfill, a GCS Prefix must be
      // provided.");
      // }
    }
    return new SchemaTransform() {
      @Override
      public @UnknownKeyFor @NonNull @Initialized PTransform<
              @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple,
              @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple>
          buildTransform() {
        PTransform<PCollection<FileIO.ReadableFile>, PCollectionTuple> deserializerTransform =
            fileDeserializer(configuration.getFormat(), configuration.getSchema());
        return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
          @Override
          public PCollectionRowTuple expand(PCollectionRowTuple input) {
            PCollectionTuple firstResult =
                input
                    .getPipeline()
                    .apply(
                        PubsubIO.readStrings()
                            .fromSubscription(configuration.getPubsubSubscription()))
                    .apply(
                        ParDo.of(new ParseNotifications())
                            .withOutputTags(
                                FILE_GENERATION_OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

            PCollectionTuple result =
                firstResult
                    .get(FILE_GENERATION_OUTPUT_TAG)
                    .apply(
                        Deduplicate.<KV<String, Long>>values()
                            .withDuration(Duration.standardMinutes(5L)))
                    .apply(Keys.create())
                    .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW))
                    .apply(FileIO.readMatches())
                    .apply(fileDeserializer(configuration.getFormat(), configuration.getSchema()));

            PCollection<Row> errors;
            if (result.has(ERROR_TAG)) {
              errors =
                  PCollectionList.of(firstResult.get(ERROR_TAG).setRowSchema(ERROR_SCHEMA))
                      .and(result.get(ERROR_TAG))
                      .apply(Flatten.pCollections());
            } else {
              errors = firstResult.get(ERROR_TAG).setRowSchema(ERROR_SCHEMA);
            }
            return PCollectionRowTuple.of(
                "output", result.get(JSON_ROW_OUTPUT_TAG), "errors", errors);
          }
        };
      }
    };
  }

  static PTransform<PCollection<FileIO.ReadableFile>, PCollectionTuple> fileDeserializer(
      String format, String schema) {
    if (format.equalsIgnoreCase("AVRO")) {
      return new PTransform<>() {
        @Override
        public PCollectionTuple expand(PCollection<FileIO.ReadableFile> input) {
          return PCollectionTuple.of(
              JSON_ROW_OUTPUT_TAG,
              input
                  .apply(AvroIO.readFilesGenericRecords(schema).withBeamSchemas(true))
                  .apply(Convert.toRows()));
        }
      };
    } else if (format.equalsIgnoreCase("JSON")) {
      Schema beamSchema = JsonUtils.beamSchemaFromJsonSchema(schema);
      SerializableFunction<String, Row> rowMapper =
          JsonUtils.getJsonStringToRowFunction(beamSchema);
      return new PTransform<>() {
        @Override
        public PCollectionTuple expand(PCollection<FileIO.ReadableFile> input) {
          PCollectionTuple result =
              input
                  .apply(TextIO.readFiles())
                  .apply(
                      ParDo.of(
                              new DoFn<String, Row>() {
                                @ProcessElement
                                public void process(
                                    @DoFn.Element String line, MultiOutputReceiver receiver) {
                                  try {
                                    receiver.get(JSON_ROW_OUTPUT_TAG).output(rowMapper.apply(line));
                                  } catch (Exception e) {
                                    receiver
                                        .get(ERROR_TAG)
                                        .output(
                                            Row.withSchema(ERROR_SCHEMA)
                                                .addValues(line, e.toString())
                                                .build());
                                  }
                                }
                              })
                          .withOutputTags(JSON_ROW_OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));
          result.get(JSON_ROW_OUTPUT_TAG).setRowSchema(beamSchema);
          result.get(ERROR_TAG).setRowSchema(ERROR_SCHEMA);
          return result;
        }
      };
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported format %s. Only supported formats are: %s",
              format, SUPPORTED_FORMATS_STR));
    }
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "syndeo:schematransform:com.google.cloud:file_stream_read:v1";
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      inputCollectionNames() {
    return List.of();
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      outputCollectionNames() {
    return List.of("output", "errors");
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class SyndeoFilesReadSchemaTransformConfiguration {
    @SchemaFieldDescription(
        "The GCS Prefix from which to consume file arrival notifications. "
            + "If one is not provided, then a Pubsub Subscription must be provided.")
    public abstract @Nullable String getGcsPrefix();

    @SchemaFieldDescription(
        "The PubSub subscription from which to consume file arrival notifications. "
            + "If one is not provided, then a GCS prefix must be provided, and the notifications will be set up "
            + "automatically.")
    public abstract @Nullable String getPubsubSubscription();

    @SchemaFieldDescription(
        "The format of the files to load. Supported formats are: " + SUPPORTED_FORMATS_STR)
    public abstract String getFormat();

    @SchemaFieldDescription(
        "The schema of the files to load."
            + " It may be an avro schema, or a json schema definition.")
    public abstract String getSchema();

    @SchemaFieldDescription(
        "If this option is set to true, ALL files within the GCS Prefix will be consumed initially "
            + "as a backfill, and then processing of the new arriving files will continue via "
            + "PubSub notifications. Default is false.")
    public abstract @Nullable Boolean getPerformBackfill();

    public static Builder builder() {
      return new AutoValue_SyndeoFilesReadSchemaTransformProvider_SyndeoFilesReadSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setGcsPrefix(String gcsPrefix);

      public abstract Builder setPubsubSubscription(String pubsubSubscription);

      public abstract Builder setFormat(String format);

      public abstract Builder setSchema(String schema);

      public abstract Builder setPerformBackfill(Boolean performBackfill);

      public abstract SyndeoFilesReadSchemaTransformConfiguration build();
    }
  }

  static class ParseNotifications extends DoFn<String, KV<String, Long>> {
    @DoFn.ProcessElement
    public void process(@DoFn.Element String fileNotification, DoFn.MultiOutputReceiver receiver) {
      ObjectMapper om = new ObjectMapper();
      try {
        JsonNode notificationPayload = om.readTree(fileNotification);
        if (!(notificationPayload.has("bucket") && notificationPayload.has("name"))) {
          throw new IllegalStateException(
              "Json message does not have the proper format. Attributes \"bucket\" "
                  + "and \"name\" are required.");
        }
        String fileName =
            "gs://"
                + Path.of(
                    notificationPayload.get("bucket").asText(),
                    notificationPayload.get("name").asText());
        receiver
            .get(FILE_GENERATION_OUTPUT_TAG)
            .output(KV.of(fileName, notificationPayload.get("generation").longValue()));
      } catch (JsonProcessingException | IllegalStateException e) {
        receiver
            .get(ERROR_TAG)
            .output(Row.withSchema(ERROR_SCHEMA).addValues(fileNotification, e.toString()).build());
      }
    }
  }
}
