/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DoFn that creates MongoDbChangeEventContext objects from FailsafeElements. */
public class CreateMongoDbChangeEventContextFn
    extends DoFn<FailsafeElement<String, String>, MongoDbChangeEventContext> {

  private static final Logger LOG =
      LoggerFactory.getLogger(CreateMongoDbChangeEventContextFn.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static TupleTag<MongoDbChangeEventContext> successfulCreationTag =
      new TupleTag<>("successfulCreation");
  public static TupleTag<FailsafeElement<String, String>> failedCreationTag =
      new TupleTag<>("failedCreation");

  private final String shadowCollectionPrefix;

  public CreateMongoDbChangeEventContextFn(String shadowCollectionPrefix) {
    this.shadowCollectionPrefix = shadowCollectionPrefix;
  }

  @ProcessElement
  public void processElement(ProcessContext context, MultiOutputReceiver out) {
    FailsafeElement<String, String> element = context.element();
    LOG.info("Creating MongoDbChangeEventContext from FailsafeElement");

    try {
      LOG.info("Parsing JSON payload");
      JsonNode jsonNode = OBJECT_MAPPER.readTree(element.getOriginalPayload());

      LOG.info("Creating MongoDbChangeEventContext with payload: {}", jsonNode);
      MongoDbChangeEventContext changeEventContext =
          new MongoDbChangeEventContext(jsonNode, shadowCollectionPrefix);

      LOG.info(
          "Successfully created MongoDbChangeEventContext for document ID: {}, collection: {}",
          changeEventContext.getDocumentId(),
          changeEventContext.getDataCollection());
      out.get(successfulCreationTag).output(changeEventContext);
    } catch (Exception e) {
      LOG.error("Error creating MongoDbChangeEventContext: {}", e.getMessage(), e);
      LOG.info("Sending failed element to DLQ");
      out.get(failedCreationTag).output(element);
    }
  }
}
