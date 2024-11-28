/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.templates.common;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.templates.common.ErrorConverters.ErrorMessage;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.Value;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/** Transforms & DoFns & Options for Teleport DatastoreIO. */
public class DatastoreConverters {

  /** Options for Reading Datastore Entities. */
  public interface DatastoreReadOptions extends PipelineOptions {
    /**
     * @deprecated Please use getFirestoreReadGqlQuery() instead.
     */
    @TemplateParameter.Text(
        order = 1,
        regexes = {"^.+$"},
        description = "GQL Query",
        helpText =
            "A GQL (https://cloud.google.com/datastore/docs/reference/gql_reference) query that specifies which entities to grab. For example, `SELECT * FROM MyKind`.")
    @Hidden
    @Deprecated
    ValueProvider<String> getDatastoreReadGqlQuery();

    /**
     * @deprecated Please use setFirestoreReadGqlQuery(value) instead.
     */
    @Hidden
    @Deprecated
    void setDatastoreReadGqlQuery(ValueProvider<String> datastoreReadGqlQuery);

    /**
     * @deprecated Please use getFirestoreReadProjectId() instead.
     */
    @TemplateParameter.ProjectId(
        order = 2,
        description = "Project ID",
        helpText =
            "The ID of the Google Cloud project that contains the Datastore instance that you want to read data from.")
    @Hidden
    @Deprecated
    ValueProvider<String> getDatastoreReadProjectId();

    /**
     * @deprecated Please use setFirestoreReadProjectId(value) instead.
     */
    @Hidden
    @Deprecated
    void setDatastoreReadProjectId(ValueProvider<String> datastoreReadProjectId);

    /**
     * @deprecated Please use getFirestoreReadNamespace() instead.
     */
    @TemplateParameter.Text(
        order = 3,
        optional = true,
        regexes = {"^[0-9A-Za-z._-]{0,100}$"},
        description = "Namespace",
        helpText =
            "The namespace of the requested entities. To use the default namespace, leave this parameter blank.")
    @Hidden
    @Deprecated
    ValueProvider<String> getDatastoreReadNamespace();

    /**
     * @deprecated Please use setFirestoreReadNamespace(value) instead.
     */
    @Hidden
    @Deprecated
    void setDatastoreReadNamespace(ValueProvider<String> datastoreReadNamespace);

    @TemplateParameter.Text(
        order = 4,
        regexes = {"^.+$"},
        description = "GQL Query",
        helpText =
            "A GQL (https://cloud.google.com/datastore/docs/reference/gql_reference) query that specifies "
                + "which entities to grab. For example, `SELECT * FROM MyKind`.")
    ValueProvider<String> getFirestoreReadGqlQuery();

    void setFirestoreReadGqlQuery(ValueProvider<String> firestoreReadGqlQuery);

    @TemplateParameter.ProjectId(
        order = 5,
        description = "Project ID",
        helpText =
            "The ID of the Google Cloud project that contains the Firestore instance that you want to read data from.")
    ValueProvider<String> getFirestoreReadProjectId();

    void setFirestoreReadProjectId(ValueProvider<String> firestoreReadProjectId);

    @TemplateParameter.Text(
        order = 6,
        optional = true,
        regexes = {"^[0-9A-Za-z._-]{0,100}$"},
        description = "Namespace",
        helpText =
            "The namespace of the requested entities. To use the default namespace, leave this parameter blank.")
    ValueProvider<String> getFirestoreReadNamespace();

    void setFirestoreReadNamespace(ValueProvider<String> firestoreReadNamespace);
  }

  /** Options for writing Datastore Entities. */
  public interface DatastoreWriteOptions extends PipelineOptions {
    /**
     * @deprecated Please use getFirestoreWriteProjectId() instead.
     */
    @TemplateParameter.ProjectId(
        order = 1,
        description = "Project ID",
        helpText = "The ID of the Google Cloud project to write the Datastore entities to.")
    @Hidden
    @Deprecated
    ValueProvider<String> getDatastoreWriteProjectId();

    /**
     * @deprecated Please use setFirestoreWriteProjectId(value) instead.
     */
    @Hidden
    @Deprecated
    void setDatastoreWriteProjectId(ValueProvider<String> datstoreWriteProjectId);

    /**
     * @deprecated Please use getFirestoreWriteEntityKind() instead.
     */
    @TemplateParameter.Text(
        order = 2,
        optional = true,
        description = "Datastore entity kind",
        helpText =
            "Datastore kind under which entities will be written in the output Google Cloud project")
    @Hidden
    @Deprecated
    ValueProvider<String> getDatastoreWriteEntityKind();

    /**
     * @deprecated Please use setFirestoreWriteEntityKind(value) instead.
     */
    @Hidden
    @Deprecated
    void setDatastoreWriteEntityKind(ValueProvider<String> value);

    /**
     * @deprecated Please use getFirestoreWriteNamespace() instead.
     */
    @TemplateParameter.Text(
        order = 3,
        optional = true,
        description = "Datastore namespace",
        helpText =
            "Datastore namespace under which entities will be written in the output Google Cloud project")
    @Hidden
    @Deprecated
    ValueProvider<String> getDatastoreWriteNamespace();

    /**
     * @deprecated Please use setFirestoreWriteNamespace(value) instead.
     */
    @Hidden
    @Deprecated
    void setDatastoreWriteNamespace(ValueProvider<String> value);

    /**
     * @deprecated Please use getFirestoreHintNumWorkers() instead.
     */
    @TemplateParameter.Integer(
        order = 4,
        optional = true,
        description = "Expected number of workers",
        helpText =
            "Hint for the expected number of workers in the Datastore ramp-up throttling step. Defaults to `500`.")
    @Default.Integer(500)
    @Hidden
    @Deprecated
    ValueProvider<Integer> getDatastoreHintNumWorkers();

    /**
     * @deprecated Please use setFirestoreHintNumWorkers(value) instead.
     */
    @Hidden
    @Deprecated
    void setDatastoreHintNumWorkers(ValueProvider<Integer> value);

    @TemplateParameter.ProjectId(
        order = 5,
        description = "Project ID",
        helpText = "The ID of the Google Cloud project to write the Firestore entities to.")
    ValueProvider<String> getFirestoreWriteProjectId();

    void setFirestoreWriteProjectId(ValueProvider<String> firestoreWriteProjectId);

    @TemplateParameter.Text(
        order = 6,
        optional = true,
        description = "Firestore entity kind",
        helpText =
            "Firestore kind under which entities will be written in the output Google Cloud project")
    ValueProvider<String> getFirestoreWriteEntityKind();

    void setFirestoreWriteEntityKind(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 7,
        optional = true,
        description = "Namespace of the Firestore entity",
        helpText =
            "Firestore namespace under which entities will be written in the output Google Cloud project")
    ValueProvider<String> getFirestoreWriteNamespace();

    void setFirestoreWriteNamespace(ValueProvider<String> value);

    @TemplateParameter.Integer(
        order = 8,
        optional = true,
        description = "Expected number of workers",
        helpText =
            "Hint for the expected number of workers in the Firestore ramp-up throttling step."
                + " The default value is `500`.")
    // @Default can not be used here as it will make it use Firestore on a Datastore template.
    ValueProvider<Integer> getFirestoreHintNumWorkers();

    void setFirestoreHintNumWorkers(ValueProvider<Integer> value);
  }

  /** Options for deleting Datastore Entities. */
  public interface DatastoreDeleteOptions extends PipelineOptions {
    /**
     * @deprecated Please use getFirestoreDeleteProjectId() instead.
     */
    @TemplateParameter.ProjectId(
        order = 1,
        description =
            "Delete all matching entities from the GQL Query present in this Datastore Project Id of",
        helpText = "Google Cloud Project Id of where to delete the datastore entities")
    @Hidden
    @Deprecated
    ValueProvider<String> getDatastoreDeleteProjectId();

    /**
     * @deprecated Please use setFirestoreDeleteProjectId(value) instead.
     */
    @Hidden
    @Deprecated
    void setDatastoreDeleteProjectId(ValueProvider<String> datastoreDeleteProjectId);

    /**
     * @deprecated Please use getFirestoreHintNumWorkers() instead.
     */
    @TemplateParameter.Integer(
        order = 2,
        optional = true,
        description = "Expected number of workers",
        helpText =
            "Hint for the expected number of workers in the Datastore ramp-up throttling step.")
    @Default.Integer(500)
    @Hidden
    @Deprecated
    ValueProvider<Integer> getDatastoreHintNumWorkers();

    /**
     * @deprecated Please use setFirestoreHintNumWorkers(value) instead.
     */
    @Hidden
    @Deprecated
    void setDatastoreHintNumWorkers(ValueProvider<Integer> value);

    @TemplateParameter.ProjectId(
        order = 3,
        description =
            "Delete all matching entities from the GQL Query present in this Firestore Project Id of",
        helpText = "Google Cloud Project Id of where to delete the firestore entities")
    ValueProvider<String> getFirestoreDeleteProjectId();

    void setFirestoreDeleteProjectId(ValueProvider<String> firestoreDeleteProjectId);

    @TemplateParameter.Integer(
        order = 4,
        optional = true,
        description = "Expected number of workers",
        helpText =
            "Hint for the expected number of workers in the Firestore ramp-up throttling step.")
    @Default.Integer(500)
    ValueProvider<Integer> getFirestoreHintNumWorkers();

    void setFirestoreHintNumWorkers(ValueProvider<Integer> value);
  }

  /** Options for reading Unique datastore Schemas. */
  public interface DatastoreReadSchemaCountOptions extends DatastoreReadOptions {}

  /** Reads Entities as JSON from Datastore. */
  @AutoValue
  public abstract static class ReadJsonEntities extends PTransform<PBegin, PCollection<String>> {
    public abstract ValueProvider<String> gqlQuery();

    public abstract ValueProvider<String> projectId();

    public abstract ValueProvider<String> namespace();

    /** Builder for ReadJsonEntities. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setGqlQuery(ValueProvider<String> gqlQuery);

      public abstract Builder setProjectId(ValueProvider<String> projectId);

      public abstract Builder setNamespace(ValueProvider<String> namespace);

      public abstract ReadJsonEntities build();
    }

    public static Builder newBuilder() {
      return new AutoValue_DatastoreConverters_ReadJsonEntities.Builder();
    }

    @Override
    public PCollection<String> expand(PBegin begin) {
      return begin
          .apply(
              "ReadFromDatastore",
              DatastoreIO.v1()
                  .read()
                  .withProjectId(projectId())
                  .withLiteralGqlQuery(gqlQuery())
                  .withNamespace(namespace()))
          .apply("EntityToJson", ParDo.of(new DatastoreConverters.EntityToJson()));
    }
  }

  /** Writes Entities encoded in JSON to Datastore. */
  @AutoValue
  public abstract static class WriteJsonEntities
      extends PTransform<PCollection<String>, PCollectionTuple> {
    public abstract ValueProvider<String> projectId();

    public abstract ValueProvider<Integer> hintNumWorkers();

    public abstract Boolean throttleRampup();

    public abstract TupleTag<String> errorTag();

    /** Builder for WriteJsonEntities. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setProjectId(ValueProvider<String> projectId);

      public abstract Builder setHintNumWorkers(ValueProvider<Integer> hintNumWorkers);

      public abstract Builder setThrottleRampup(Boolean throttleRampup);

      public abstract Builder setErrorTag(TupleTag<String> errorTag);

      public abstract WriteJsonEntities build();
    }

    public static Builder newBuilder() {
      return new AutoValue_DatastoreConverters_WriteJsonEntities.Builder()
          .setHintNumWorkers(StaticValueProvider.of(500))
          .setThrottleRampup(true); // defaults
    }

    @Override
    public PCollectionTuple expand(PCollection<String> entityJson) {
      TupleTag<Entity> goodTag = new TupleTag<>();
      DatastoreV1.Write datastoreWrite =
          DatastoreIO.v1().write().withProjectId(projectId()).withHintNumWorkers(hintNumWorkers());
      if (!throttleRampup()) {
        datastoreWrite = datastoreWrite.withRampupThrottlingDisabled();
      }

      PCollectionTuple entities =
          entityJson
              .apply("StringToEntity", ParDo.of(new JsonToEntity()))
              .apply(
                  "CheckSameKey",
                  CheckSameKey.newBuilder().setErrorTag(errorTag()).setGoodTag(goodTag).build());

      entities.get(goodTag).apply("WriteToDatastore", datastoreWrite);
      return entities;
    }
  }

  /** Removes any entities that have the same key, and throws exception. */
  @AutoValue
  public abstract static class CheckSameKey
      extends PTransform<PCollection<Entity>, PCollectionTuple> {
    public abstract TupleTag<Entity> goodTag();

    public abstract TupleTag<String> errorTag();

    private Counter duplicatedKeys = Metrics.counter(CheckSameKey.class, "duplicated-keys");

    /** Builder for {@link CheckSameKey}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setGoodTag(TupleTag<Entity> goodTag);

      public abstract Builder setErrorTag(TupleTag<String> errorTag);

      public abstract CheckSameKey build();
    }

    public static Builder newBuilder() {
      return new AutoValue_DatastoreConverters_CheckSameKey.Builder();
    }

    /** Handles detecting entities that have duplicate keys that are non equal. */
    @Override
    public PCollectionTuple expand(PCollection<Entity> entities) {
      return entities
          .apply(
              "ExposeKeys",
              ParDo.of(
                  new DoFn<Entity, KV<byte[], Entity>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws IOException {
                      Entity e = c.element();

                      // Serialize Key deterministically
                      ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
                      CodedOutputStream output = CodedOutputStream.newInstance(byteOutputStream);
                      output.useDeterministicSerialization();
                      c.element().getKey().writeTo(output);
                      output.flush();

                      c.output(KV.of(byteOutputStream.toByteArray(), e));
                    }
                  }))
          .apply(GroupByKey.create())
          .apply(
              "ErrorOnDuplicateKeys",
              ParDo.of(
                      new DoFn<KV<byte[], Iterable<Entity>>, Entity>() {
                        private EntityJsonPrinter entityJsonPrinter;

                        @Setup
                        public void setup() {
                          entityJsonPrinter = new EntityJsonPrinter();
                        }

                        @ProcessElement
                        public void processElement(ProcessContext c) throws IOException {
                          Iterator<Entity> entities = c.element().getValue().iterator();
                          Entity entity = entities.next();
                          if (entities.hasNext()) {
                            do {
                              duplicatedKeys.inc();
                              ErrorMessage errorMessage =
                                  ErrorMessage.newBuilder()
                                      .setMessage("Duplicate Datastore Key")
                                      .setData(entityJsonPrinter.print(entity))
                                      .build();
                              c.output(errorTag(), errorMessage.toJson());
                              entity = entities.hasNext() ? entities.next() : null;
                            } while (entity != null);
                          } else {
                            c.output(entity);
                          }
                        }
                      })
                  .withOutputTags(goodTag(), TupleTagList.of(errorTag())));
    }
  }

  /** Deletes matching entities. */
  @AutoValue
  public abstract static class DatastoreDeleteEntityJson
      extends PTransform<PCollection<String>, PDone> {
    public abstract ValueProvider<String> projectId();

    public abstract ValueProvider<Integer> hintNumWorkers();

    public abstract Boolean throttleRampup();

    /** Builder for DatastoreDeleteEntityJson. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setProjectId(ValueProvider<String> projectId);

      public abstract Builder setHintNumWorkers(ValueProvider<Integer> hintNumWorkers);

      public abstract Builder setThrottleRampup(Boolean throttleRampup);

      public abstract DatastoreDeleteEntityJson build();
    }

    public static Builder newBuilder() {
      return new AutoValue_DatastoreConverters_DatastoreDeleteEntityJson.Builder()
          .setHintNumWorkers(StaticValueProvider.of(500))
          .setThrottleRampup(true); // defaults
    }

    @Override
    public PDone expand(PCollection<String> entityJson) {
      DatastoreV1.DeleteKey datastoreDelete =
          DatastoreIO.v1()
              .deleteKey()
              .withProjectId(projectId())
              .withHintNumWorkers(hintNumWorkers());
      if (!throttleRampup()) {
        datastoreDelete = datastoreDelete.withRampupThrottlingDisabled();
      }
      return entityJson
          .apply("StringToKey", ParDo.of(new JsonToKey()))
          .apply("DeleteKeys", datastoreDelete);
    }
  }

  /** Gets all the unique datastore schemas in collection. */
  @AutoValue
  public abstract static class DatastoreReadSchemaCount
      extends PTransform<PBegin, PCollection<String>> {
    public abstract ValueProvider<String> projectId();

    public abstract ValueProvider<String> gqlQuery();

    public abstract ValueProvider<String> namespace();

    /** Builder for DatastoreReadSchemaCount. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setGqlQuery(ValueProvider<String> gqlQuery);

      public abstract Builder setProjectId(ValueProvider<String> projectId);

      public abstract Builder setNamespace(ValueProvider<String> namespace);

      public abstract DatastoreReadSchemaCount build();
    }

    public static Builder newBuilder() {
      return new AutoValue_DatastoreConverters_DatastoreReadSchemaCount.Builder();
    }

    @Override
    public PCollection<String> expand(PBegin begin) {
      return begin
          .apply(
              "ReadFromDatastore",
              DatastoreIO.v1()
                  .read()
                  .withProjectId(projectId())
                  .withLiteralGqlQuery(gqlQuery())
                  .withNamespace(namespace()))
          .apply("ParseEntitySchema", ParDo.of(new EntityToSchemaJson()))
          .apply("CountUniqueSchemas", Count.<String>perElement())
          .apply(
              "Jsonify",
              ParDo.of(
                  new DoFn<KV<String, Long>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      JsonObject out = new JsonObject();
                      out.addProperty("schema", c.element().getKey());
                      out.addProperty("count", c.element().getValue());
                      c.output(out.toString());
                    }
                  }));
    }
  }

  /**
   * DoFn for converting a Datastore Entity to JSON. Json in mapped using protov3:
   * https://developers.google.com/protocol-buffers/docs/proto3#json
   */
  public static class EntityToJson extends DoFn<Entity, String> {
    private EntityJsonPrinter entityJsonPrinter;

    @Setup
    public void setup() {
      entityJsonPrinter = new EntityJsonPrinter();
    }

    /** Processes Datastore entity into json. */
    @ProcessElement
    public void processElement(ProcessContext c) throws InvalidProtocolBufferException {
      Entity entity = c.element();
      String entityJson = entityJsonPrinter.print(entity);
      c.output(entityJson);
    }
  }

  /**
   * DoFn for converting a Protov3 JSON Encoded Entity to a Datastore Entity. JSON in mapped
   * protov3: https://developers.google.com/protocol-buffers/docs/proto3#json
   */
  public static class JsonToEntity extends DoFn<String, Entity> {
    private EntityJsonParser entityJsonParser;

    @Setup
    public void setup() {
      entityJsonParser = new EntityJsonParser();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws InvalidProtocolBufferException {
      String entityJson = c.element();
      Entity.Builder entityBuilder = Entity.newBuilder();
      entityJsonParser.merge(entityJson, entityBuilder);

      // Build entity who's key has an empty project Id.
      // This allows DatastoreIO to handle what project Entities are loaded into
      com.google.datastore.v1.Key k = entityBuilder.build().getKey();
      entityBuilder.setKey(
          com.google.datastore.v1.Key.newBuilder()
              .addAllPath(k.getPathList())
              .setPartitionId(
                  PartitionId.newBuilder()
                      .setProjectId("")
                      .setNamespaceId(k.getPartitionId().getNamespaceId())));

      c.output(entityBuilder.build());
    }
  }

  /**
   * DoFn for converting a Protov3 Json Encoded Entity and extracting its key. Expects json in
   * format of: { key: { "partitionId": {"projectId": "", "namespace": ""}, "path": [ {"kind": "",
   * "id": "", "name": ""}]}
   */
  public static class JsonToKey extends DoFn<String, Key> {
    private EntityJsonParser entityJsonParser;

    @Setup
    public void setup() {
      entityJsonParser = new EntityJsonParser();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws InvalidProtocolBufferException {
      String entityJson = c.element();
      Entity e = entityJsonParser.parse(entityJson);
      c.output(e.getKey());
    }
  }

  /** DoFn for extracting the Schema of a Entity. */
  public static class EntityToSchemaJson extends DoFn<Entity, String> {

    /**
     * Grabs the schema for what data is in the Array.
     *
     * @param arrayValue a populated array
     * @return a schema of what kind of data is in the array
     */
    private JsonArray arraySchema(ArrayValue arrayValue) {
      Set<JsonObject> entities = new HashSet<>();
      Set<String> primitives = new HashSet<>();
      Set<JsonArray> subArrays = new HashSet<>();

      arrayValue.getValuesList().stream()
          .forEach(
              value -> {
                switch (value.getValueTypeCase()) {
                  case ENTITY_VALUE:
                    entities.add(entitySchema(value.getEntityValue()));
                    break;
                  case ARRAY_VALUE:
                    subArrays.add(arraySchema(value.getArrayValue()));
                    break;
                  default:
                    primitives.add(value.getValueTypeCase().toString());
                    break;
                }
              });

      JsonArray jsonArray = new JsonArray();
      entities.stream().forEach(jsonArray::add);
      primitives.stream().forEach(jsonArray::add);
      subArrays.stream().forEach(jsonArray::add);
      return jsonArray;
    }

    /**
     * Grabs the schema for what data is in the Entity.
     *
     * @param entity a populated entity
     * @return a schema of what kind of data is in the entity
     */
    private JsonObject entitySchema(Entity entity) {
      JsonObject jsonObject = new JsonObject();
      entity.getPropertiesMap().entrySet().stream()
          .forEach(
              entrySet -> {
                String key = entrySet.getKey();
                Value value = entrySet.getValue();
                switch (value.getValueTypeCase()) {
                  case ENTITY_VALUE:
                    jsonObject.add(key, entitySchema(value.getEntityValue()));
                    break;
                  case ARRAY_VALUE:
                    jsonObject.add(key, arraySchema(value.getArrayValue()));
                    break;
                  default:
                    jsonObject.addProperty(key, value.getValueTypeCase().toString());
                }
              });
      return jsonObject;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      Entity entity = c.element();
      JsonObject schema = entitySchema(entity);
      c.output(schema.toString());
    }
  }

  /** Converts an Entity to a JSON String. */
  public static class EntityJsonPrinter {

    // A cached jsonPrinter
    private JsonFormat.Printer jsonPrinter;

    public EntityJsonPrinter() {
      TypeRegistry typeRegistry = TypeRegistry.newBuilder().add(Entity.getDescriptor()).build();
      jsonPrinter =
          JsonFormat.printer().usingTypeRegistry(typeRegistry).omittingInsignificantWhitespace();
    }

    /**
     * Prints an Entity as a JSON String.
     *
     * @param entity a Datastore Protobuf Entity.
     * @return Datastore Entity encoded as a JSON String.
     * @throws InvalidProtocolBufferException
     */
    public String print(Entity entity) throws InvalidProtocolBufferException {
      return jsonPrinter.print(entity);
    }
  }

  /** Converts a JSON String to an Entity. */
  public static class EntityJsonParser {

    // A cached jsonParser
    private final JsonFormat.Parser jsonParser;

    public EntityJsonParser() {
      TypeRegistry typeRegistry = TypeRegistry.newBuilder().add(Entity.getDescriptor()).build();

      jsonParser = JsonFormat.parser().usingTypeRegistry(typeRegistry);
    }

    public void merge(String json, Entity.Builder entityBuilder)
        throws InvalidProtocolBufferException {
      jsonParser.merge(json, entityBuilder);
    }

    public Entity parse(String json) throws InvalidProtocolBufferException {
      Entity.Builder entityBuilder = Entity.newBuilder();
      merge(json, entityBuilder);
      return entityBuilder.build();
    }
  }

  /** Writes Entities to Datastore. */
  @AutoValue
  public abstract static class WriteEntities
      extends PTransform<PCollection<Entity>, PCollectionTuple> {
    public abstract ValueProvider<String> projectId();

    public abstract ValueProvider<Integer> hintNumWorkers();

    public abstract Boolean throttleRampup();

    public abstract TupleTag<String> errorTag();

    /** Builder for WriteJsonEntities. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setProjectId(ValueProvider<String> projectId);

      public abstract Builder setHintNumWorkers(ValueProvider<Integer> hintNumWorkers);

      public abstract Builder setThrottleRampup(Boolean throttleRampup);

      public abstract Builder setErrorTag(TupleTag<String> errorTag);

      public abstract WriteEntities build();
    }

    public static Builder newBuilder() {
      return new AutoValue_DatastoreConverters_WriteEntities.Builder()
          .setHintNumWorkers(StaticValueProvider.of(500))
          .setThrottleRampup(true); // defaults
    }

    @Override
    public PCollectionTuple expand(PCollection<Entity> entity) {
      TupleTag<Entity> goodTag = new TupleTag<>();
      DatastoreV1.Write datastoreWrite =
          DatastoreIO.v1().write().withProjectId(projectId()).withHintNumWorkers(hintNumWorkers());
      if (!throttleRampup()) {
        datastoreWrite = datastoreWrite.withRampupThrottlingDisabled();
      }

      // Due to the fact that DatastoreIO does non-transactional writing to Datastore, writing the
      // same entity more than once in the same commit is not supported (error "A non-transactional
      // commit may not contain multiple mutations affecting the same entity). Messages with the
      // same key are thus not written to Datastore and instead routed to an error PCollection for
      // further handling downstream.
      PCollectionTuple entities =
          entity.apply(
              "CheckSameKey",
              CheckSameKey.newBuilder().setErrorTag(errorTag()).setGoodTag(goodTag).build());
      entities.get(goodTag).apply("WriteToDatastore", datastoreWrite);
      return entities;
    }
  }

  /** Removes any invalid entities that don't have a key. */
  @AutoValue
  public abstract static class CheckNoKey
      extends PTransform<PCollection<Entity>, PCollectionTuple> {

    abstract TupleTag<Entity> successTag();

    abstract TupleTag<String> failureTag();

    /** Builder for {@link CheckNoKey}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setSuccessTag(TupleTag<Entity> successTag);

      public abstract Builder setFailureTag(TupleTag<String> failureTag);

      public abstract CheckNoKey build();
    }

    public static Builder newBuilder() {
      return new AutoValue_DatastoreConverters_CheckNoKey.Builder();
    }

    @Override
    public PCollectionTuple expand(PCollection<Entity> entities) {
      return entities.apply(
          "DetectInvalidEntities",
          ParDo.of(
                  new DoFn<Entity, Entity>() {
                    private EntityJsonPrinter entityJsonPrinter;

                    @Setup
                    public void setup() {
                      entityJsonPrinter = new EntityJsonPrinter();
                    }

                    @ProcessElement
                    public void processElement(ProcessContext c) throws IOException {
                      Entity entity = c.element();
                      if (entity.hasKey()) {
                        c.output(entity);
                      } else {
                        ErrorMessage errorMessage =
                            ErrorMessage.newBuilder()
                                .setMessage("Datastore Entity Without Key")
                                .setData(entityJsonPrinter.print(entity))
                                .build();
                        c.output(failureTag(), errorMessage.toJson());
                      }
                    }
                  })
              .withOutputTags(successTag(), TupleTagList.of(failureTag())));
    }
  }
}
