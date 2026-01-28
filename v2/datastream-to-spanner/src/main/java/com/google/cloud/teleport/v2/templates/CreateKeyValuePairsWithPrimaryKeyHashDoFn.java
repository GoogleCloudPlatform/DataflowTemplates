/*
 * Copyright (C) 2026 Google LLC
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

import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.CONVERSION_ERRORS_COUNTER_NAME;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventSpannerConvertor;
import com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventConvertor;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateKeyValuePairsWithPrimaryKeyHashDoFn
    extends DoFn<FailsafeElement<String, String>, KV<Long, FailsafeElement<String, String>>> {

  private static final Logger LOG =
      LoggerFactory.getLogger(CreateKeyValuePairsWithPrimaryKeyHashDoFn.class);

  private final PCollectionView<Ddl> ddlView;

  // Jackson Object mapper.
  private transient ObjectMapper mapper;

  private final Counter conversionErrors =
      Metrics.counter(SpannerTransactionWriterDoFn.class, CONVERSION_ERRORS_COUNTER_NAME);

  public CreateKeyValuePairsWithPrimaryKeyHashDoFn(PCollectionView<Ddl> ddlView) {
    this.ddlView = ddlView;
  }

  @Setup
  public void setup() {
    mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    FailsafeElement<String, String> msg = c.element();
    String tableName = "";
    try {
      JsonNode changeEvent = mapper.readTree(msg.getPayload());
      Ddl ddl = c.sideInput(ddlView);

      tableName = changeEvent.get(DatastreamConstants.EVENT_TABLE_NAME_KEY).asText();
      ChangeEventConvertor.convertChangeEventColumnKeysToLowerCase(changeEvent);
      ChangeEventConvertor.verifySpannerSchema(ddl, changeEvent);
      com.google.cloud.spanner.Key primaryKey =
          ChangeEventSpannerConvertor.changeEventToPrimaryKey(
              tableName, ddl, changeEvent, /* convertNameToLowerCase= */ true);
      String finalKeyString = tableName + "_" + primaryKey.toString();
      Long finalKey = (long) finalKeyString.hashCode();
      c.output(KV.of(finalKey, msg));
    } catch (Exception e) {
      LOG.error(
          "Error while converting change event to primary key hash for tableName=" + tableName, e);
      // Errors that result during Event conversions are not retryable.
      // Making a copy, as the input must not be mutated.
      FailsafeElement<String, String> output = FailsafeElement.of(msg);
      output.setErrorMessage(e.getMessage());
      c.output(DatastreamToSpannerConstants.PERMANENT_ERROR_TAG, output);
      conversionErrors.inc();
    }
  }
}
