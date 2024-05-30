/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates.transform;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.EVENT_SCHEMA_KEY;
import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_CHANGE_TYPE_KEY;
import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_TABLE_NAME_KEY;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.spanner.exceptions.TransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventSessionConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventToMapConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.DroppedTableException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.TransformationContext;
import com.google.cloud.teleport.v2.spanner.migrations.utils.CustomTransformationImplFetcher;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.cloud.teleport.v2.templates.SpannerTransactionWriterDoFn;
import com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class ChangeEventTransformerDoFn
    extends DoFn<FailsafeElement<String, String>, FailsafeElement<String, String>>
    implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ChangeEventTransformerDoFn.class);

  // Jackson Object mapper.
  private transient ObjectMapper mapper;

  private ISpannerMigrationTransformer datastreamToSpannerTransformer;

  // ChangeEventSessionConvertor utility object.
  private ChangeEventSessionConvertor changeEventSessionConvertor;

  @Nullable
  public abstract Schema schema();

  @Nullable
  public abstract TransformationContext transformationContext();

  public abstract String sourceType();

  @Nullable
  public abstract CustomTransformation customTransformation();

  private final Counter processedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Total events processed");

  private final Counter filteredEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Filtered events");

  private final Counter transformedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Transformed events");

  private final Counter skippedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Skipped events");
  private final Counter failedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Other permanent errors");

  private final Counter customTransformationException =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Custom Transformation Exceptions");

  private final Distribution applyCustomTransformationResponseTimeMetric =
      Metrics.distribution(
          SpannerTransactionWriterDoFn.class, "apply_custom_transformation_impl_latency_ms");

  public static ChangeEventTransformerDoFn create(
      Schema schema,
      TransformationContext transformationContext,
      String sourceType,
      CustomTransformation customTransformation) {
    return new AutoValue_ChangeEventTransformerDoFn(
        schema, transformationContext, sourceType, customTransformation);
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() {
    mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    datastreamToSpannerTransformer =
        CustomTransformationImplFetcher.getCustomTransformationLogicImpl(customTransformation());
    changeEventSessionConvertor =
        new ChangeEventSessionConvertor(schema(), transformationContext(), sourceType());
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    FailsafeElement<String, String> msg = c.element();
    processedEvents.inc();

    try {

      JsonNode changeEvent = mapper.readTree(msg.getPayload());

      String tableName = changeEvent.get(EVENT_TABLE_NAME_KEY).asText();
      Map<String, Object> sourceRecord =
          ChangeEventToMapConvertor.convertChangeEventToMap(changeEvent);
      String shardId = "";
      if (!schema().isEmpty()) {
        schema().verifyTableInSession(changeEvent.get(EVENT_TABLE_NAME_KEY).asText());
        changeEvent = changeEventSessionConvertor.transformChangeEventViaSessionFile(changeEvent);
      }
      if (transformationContext() != null) {
        Map<String, String> schemaToShardId = transformationContext().getSchemaToShardId();
        String schemaName = changeEvent.get(EVENT_SCHEMA_KEY).asText();
        shardId = schemaToShardId.get(schemaName);
      }

      if (datastreamToSpannerTransformer != null) {
        Instant startTimestamp = Instant.now();
        MigrationTransformationRequest migrationTransformationRequest =
            new MigrationTransformationRequest(
                tableName, sourceRecord, shardId, changeEvent.get(EVENT_CHANGE_TYPE_KEY).asText());
        MigrationTransformationResponse migrationTransformationResponse =
            datastreamToSpannerTransformer.toSpannerRow(migrationTransformationRequest);
        Instant endTimestamp = Instant.now();
        applyCustomTransformationResponseTimeMetric.update(
            new Duration(startTimestamp, endTimestamp).getMillis());
        if (migrationTransformationResponse.isEventFiltered()) {
          filteredEvents.inc();
          c.output(DatastreamToSpannerConstants.FILTERED_EVENT_TAG, msg.getPayload());
          return;
        }
        changeEvent =
            ChangeEventToMapConvertor.transformChangeEventViaCustomTransformation(
                changeEvent, migrationTransformationResponse.getResponseRow());
      }
      transformedEvents.inc();
      c.output(
          DatastreamToSpannerConstants.TRANSFORMED_EVENT_TAG,
          FailsafeElement.of(changeEvent.toString(), changeEvent.toString()));
    } catch (DroppedTableException e) {
      // Errors when table exists in source but was dropped during conversion. We do not output any
      // errors to dlq for this.
      LOG.warn(e.getMessage());
      skippedEvents.inc();
    } catch (TransformationException e) {
      // Errors that result from the custom JAR during transformation are not retryable.
      outputWithErrorTag(c, msg, e, DatastreamToSpannerConstants.PERMANENT_ERROR_TAG);
      customTransformationException.inc();
    } catch (InvalidChangeEventException e) {
      // Errors that result from invalid change events.
      outputWithErrorTag(c, msg, e, DatastreamToSpannerConstants.PERMANENT_ERROR_TAG);
      skippedEvents.inc();
    } catch (Exception e) {
      // Any other errors are considered severe and not retryable.
      outputWithErrorTag(c, msg, e, DatastreamToSpannerConstants.PERMANENT_ERROR_TAG);
      failedEvents.inc();
    }
  }

  void outputWithErrorTag(
      ProcessContext c,
      FailsafeElement<String, String> changeEvent,
      Exception e,
      TupleTag<FailsafeElement<String, String>> errorTag) {
    // Making a copy, as the input must not be mutated.
    FailsafeElement<String, String> output = FailsafeElement.of(changeEvent);
    output.setErrorMessage(e.getMessage());
    c.output(errorTag, output);
  }

  public void setMapper(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public void setChangeEventSessionConvertor(
      ChangeEventSessionConvertor changeEventSessionConvertor) {
    this.changeEventSessionConvertor = changeEventSessionConvertor;
  }

  public void setDatastreamToSpannerTransformer(
      ISpannerMigrationTransformer datastreamToSpannerTransformer) {
    this.datastreamToSpannerTransformer = datastreamToSpannerTransformer;
  }
}
