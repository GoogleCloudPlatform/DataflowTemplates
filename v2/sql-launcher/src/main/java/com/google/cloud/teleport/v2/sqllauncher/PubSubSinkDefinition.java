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
package com.google.cloud.teleport.v2.sqllauncher;

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog.DataCatalogPipelineOptions;
import org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog.DataCatalogTableProvider;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubGrpcClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.DropFields;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Options to configure and create a PubSub topic output.
 *
 * <p>Parses JSON of the form:
 *
 * <pre>
 *   {
 *    "type": "pubsub",
 *    "projectId": "..",
 *    "topic": "..",
 *    "create_disposition": "CREATE_IF_NOT_FOUND|FAIL_IF_NOT_FOUND",
 *   }
 * </pre>
 *
 * <p>{@link #createTransform(PCollection, DataflowSqlLauncherOptions, DataCatalogTableProvider)}
 * verifies that the schema of the topic is compatible with given {@code PCollection<Row>} and
 * creates a {@link org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.Write} transform to write to it.
 */
class PubSubSinkDefinition extends SinkDefinition {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubSinkDefinition.class);

  public enum CreateDisposition {
    CREATE_IF_NOT_FOUND,
    FAIL_IF_NOT_FOUND;
  }

  private final String projectId;
  private final String topic;
  private final CreateDisposition createDisposition;

  @JsonCreator
  public PubSubSinkDefinition(
      @JsonProperty(value = "projectId", required = true) String projectId,
      @JsonProperty(value = "topic", required = true) String topic,
      @JsonProperty(value = "createDisposition", required = true)
          CreateDisposition createDisposition) {
    this.projectId = projectId;
    this.topic = topic;
    this.createDisposition = createDisposition;
  }

  public String getProjectId() {
    return projectId;
  }

  public String getTopic() {
    return topic;
  }

  public CreateDisposition getCreateDisposition() {
    return createDisposition;
  }

  private String getFullTopic() {
    return "projects/" + projectId + "/topics/" + topic;
  }

  @Override
  String getTableName() {
    return String.format("pubsub.topic.`%s`.`%s`", projectId, topic);
  }

  @Override
  protected PTransform<PCollection<Row>, POutput> createTransform(
      PCollection<Row> queryResult,
      DataflowSqlLauncherOptions options,
      DataCatalogTableProvider tableProvider) {
    // Check if topic exists
    boolean topicExists;
    boolean topicCreated = false;
    try (PubsubClient pubsub =
        PubsubGrpcClient.FACTORY.newClient(null, null, options.as(PubsubOptions.class))) {
      try {
        topicExists =
            pubsub.listTopics(PubsubClient.projectPathFromId(projectId)).stream()
                .anyMatch(topicPath -> topicPath.getName().equals(topic));
      } catch (IOException e) {
        throw new RuntimeException("Failed to verify that topic exists", e);
      }

      if (!topicExists) {
        switch (this.createDisposition) {
          case FAIL_IF_NOT_FOUND:
            throw new InvalidSinkException(
                String.format("Pubsub topic `%s` does not exist.", getFullTopic()));
          case CREATE_IF_NOT_FOUND:
            // create topic and set schema
            try {
              pubsub.createTopic(PubsubClient.topicPathFromName(projectId, topic));
            } catch (IOException e) {
              throw new RuntimeException("Failed to create topic", e);
            }
            topicCreated = true;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to create pubsub client", e);
    }

    boolean schemaApplied;
    try (DataCatalogTableProvider client =
        DataCatalogTableProvider.create(options.as(DataCatalogPipelineOptions.class))) {
      schemaApplied =
          client.setSchemaIfNotPresent(
              getTableName(), schemaWithTimestamp(queryResult.getSchema()));
    } catch (UnsupportedOperationException e) {
      // It should not be possible for a query to produce an output schema that we can't
      // convert to Data Catalog, so this is an AssertionError.
      throw new AssertionError(
          String.format(
              "Unable to convert the query's output schema <%s> to a Data Catalog schema.",
              queryResult.getSchema()),
          e);
    }

    // If the topic we just created already has a schema something is wrong, and the query will
    // likely fail. Bail out now.
    if (topicCreated && !schemaApplied) {
      throw new AssertionError(
          String.format(
              "Unable to set schema for newly created topic '%s' because it already has one.",
              getFullTopic()));
    }

    Table outputTable;
    try {
      // Throws UnsupportedOperationException if topic doesn't have a schema or there was an issue
      // parsing it, returns null if data catalog entry doesn't exist
      outputTable = tableProvider.getTable(getTableName());
    } catch (UnsupportedOperationException e) {
      throw new UnsupportedOperationException(
          String.format(
              "There's a problem with the schema for `%s`: %s", getTableName(), e.getMessage()),
          e);
    }

    if (!schemaMinusTimestamp(queryResult.getSchema())
        .assignableTo(schemaMinusTimestamp(outputTable.getSchema()))) {
      throw new InvalidSinkException(
          String.format(
              "PubSub topic `%s` has schema <%s>, which is not compatible with the schema of this"
                  + " query's output <%s>",
              getFullTopic(), outputTable.getSchema(), queryResult.getSchema()));
    }

    // TODO: Use tableProvider to create the sink
    return new PubSubJsonSink();
  }

  private class PubSubJsonSink extends PTransform<PCollection<Row>, POutput> {
    public POutput expand(PCollection<Row> input) {
      PCollection<Row> rows;
      if (input.getSchema().hasField("event_timestamp")) {
        LOG.warn(
            "Dropping output field \"event_timestamp\" before writing to `%s`. This is a "
                + "read-only field that is populated with Pub/Sub message publish time.",
            getTableName());
        rows = input.apply(DropFields.fields("event_timestamp"));
      } else {
        rows = input;
      }
      return rows.apply(ToJson.<Row>of()).apply(PubsubIO.writeStrings().to(getFullTopic()));
    }
  }

  private Schema schemaMinusTimestamp(Schema schema) {
    return new Schema(
        schema.getFields().stream()
            .filter(f -> !f.getName().equals("event_timestamp"))
            .collect(toList()));
  }

  private Schema schemaWithTimestamp(Schema schema) {
    if (schema.hasField("event_timestamp")) {
      return schema;
    }

    List<Schema.Field> fields = new ArrayList<>(schema.getFields());
    fields.add(Schema.Field.of("event_timestamp", Schema.FieldType.DATETIME));
    return new Schema(fields);
  }
}
