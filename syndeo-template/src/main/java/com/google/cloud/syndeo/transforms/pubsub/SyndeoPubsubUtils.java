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
package com.google.cloud.syndeo.transforms.pubsub;

import com.google.cloud.pubsub.v1.SchemaServiceClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.syndeo.common.ProtoSchemaUtils;
import com.google.pubsub.v1.Encoding;
import com.google.pubsub.v1.Schema;
import com.google.pubsub.v1.Topic;
import java.io.IOException;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;

/** A utility class for working with Google Cloud Pub/Sub topics and subscriptions. */
public class SyndeoPubsubUtils {

  /**
   * Retrieves the schema and data conversion function for a Pub/Sub topic.
   *
   * @param topicName the name of the Pub/Sub topic
   * @param subscriptionName the name of the subscription to the topic (optional)
   * @return a {@link KV} object containing the schema and data conversion function
   * @throws IOException if an error occurs while retrieving the schema from Pub/Sub
   * @throws IllegalArgumentException if the schema or encoding is not supported
   */
  public static KV<org.apache.beam.sdk.schemas.Schema, SerializableFunction<byte[], Row>>
      getTopicInfo(String topicName, String subscriptionName) throws IOException {
    if (subscriptionName != null) {
      try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
        topicName = subscriptionAdminClient.getSubscription(subscriptionName).getTopic();
      }
    }
    try (TopicAdminClient client = TopicAdminClient.create()) {
      Topic topicMetadata = client.getTopic(topicName);
      if (!topicMetadata.hasSchemaSettings()) {
        throw new IllegalArgumentException(
            String.format(
                "A schema definition for topic %s was expected, but none was found. "
                    + "Please provide a schema as a configuration parameter instead, or assign a "
                    + "schema to the topic.",
                topicName));
      }
      Encoding topicDataEncoding = topicMetadata.getSchemaSettings().getEncoding();
      com.google.pubsub.v1.Schema fullSchema =
          getFullSchema(topicMetadata.getSchemaSettings().getSchema());
      if (topicDataEncoding == Encoding.BINARY && fullSchema.getType() == Schema.Type.AVRO) {
        org.apache.beam.sdk.schemas.Schema beamSchema =
            AvroUtils.toBeamSchema(
                new org.apache.avro.Schema.Parser().parse(fullSchema.getDefinition()));
        return KV.of(beamSchema, AvroUtils.getAvroBytesToRowFunction(beamSchema));
      } else if (topicDataEncoding == Encoding.JSON && fullSchema.getType() == Schema.Type.AVRO) {
        org.apache.beam.sdk.schemas.Schema beamSchema =
            AvroUtils.toBeamSchema(
                new org.apache.avro.Schema.Parser().parse(fullSchema.getDefinition()));
        return KV.of(beamSchema, JsonUtils.getJsonBytesToRowFunction(beamSchema));
      } else if (topicDataEncoding == Encoding.JSON
          && fullSchema.getType() == Schema.Type.PROTOCOL_BUFFER) {
        org.apache.beam.sdk.schemas.Schema beamSchema =
            ProtoSchemaUtils.beamSchemaFromProtoSchemaDescription(fullSchema.getDefinition());
        return KV.of(beamSchema, JsonUtils.getJsonBytesToRowFunction(beamSchema));
      } else {
        throw new IllegalArgumentException(
            String.format(
                "The schema for topic %s in Pubsub's schema service is of type %s, "
                    + "and the encoding is %s. At this moment, only AVRO schemas with BINARY or "
                    + "JSON encoding, or PROTOBUF schemas with JSON encoding are supported.",
                topicName, fullSchema.getType(), topicDataEncoding));
      }
    } catch (ProtoSchemaUtils.SyndeoSchemaParseException e) {
      throw new IllegalArgumentException("Unable to parse input schema for topic " + topicName, e);
    }
  }

  /**
   * Retrieves the full schema definition for a schema name.
   *
   * @param schemaName the name of the schema
   * @return a {@link com.google.pubsub.v1.Schema} object representing the full schema definition
   * @throws IOException if an error occurs while retrieving the schema from Pub/Sub
   */
  private static com.google.pubsub.v1.Schema getFullSchema(String schemaName) throws IOException {
    try (SchemaServiceClient client = SchemaServiceClient.create()) {
      return client.getSchema(schemaName);
    }
  }
}
