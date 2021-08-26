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
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
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
    @Description("GQL Query which specifies what entities to grab")
    ValueProvider<String> getDatastoreReadGqlQuery();

    void setDatastoreReadGqlQuery(ValueProvider<String> datastoreReadGqlQuery);

    @Description("GCP Project Id of where the datastore entities live")
    ValueProvider<String> getDatastoreReadProjectId();

    void setDatastoreReadProjectId(ValueProvider<String> datastoreReadProjectId);

    @Description("Namespace of requested Entties. Set as \"\" for default namespace")
    ValueProvider<String> getDatastoreReadNamespace();

    void setDatastoreReadNamespace(ValueProvider<String> datstoreReadNamespace);
  }

  /** Options for writing Datastore Entities. */
  public interface DatastoreWriteOptions extends PipelineOptions {
    @Description("GCP Project Id of where to write the datastore entities")
    ValueProvider<String> getDatastoreWriteProjectId();

    void setDatastoreWriteProjectId(ValueProvider<String> datstoreWriteProjectId);

    @Description("Kind of the Datastore entity")
    ValueProvider<String> getDatastoreWriteEntityKind();

    void setDatastoreWriteEntityKind(ValueProvider<String> value);

    @Description("Namespace of the Datastore entity")
    ValueProvider<String> getDatastoreWriteNamespace();

    void setDatastoreWriteNamespace(ValueProvider<String> value);
  }

  /** Options for deleting Datastore Entities. */
  public interface DatastoreDeleteOptions extends PipelineOptions {
    @Description("GCP Project Id of where to delete the datastore entities")
    ValueProvider<String> getDatastoreDeleteProjectId();

    void setDatastoreDeleteProjectId(ValueProvider<String> datastoreDeleteProjectId);
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

    public abstract TupleTag<String> errorTag();

    /** Builder for WriteJsonEntities. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setProjectId(ValueProvider<String> projectId);

      public abstract Builder setErrorTag(TupleTag<String> errorTag);

      public abstract WriteJsonEntities build();
    }

    public static Builder newBuilder() {
      return new AutoValue_DatastoreConverters_WriteJsonEntities.Builder();
    }

    @Override
    public PCollectionTuple expand(PCollection<String> entityJson) {
      TupleTag<Entity> goodTag = new TupleTag<>();

      PCollectionTuple entities =
          entityJson
              .apply("StringToEntity", ParDo.of(new JsonToEntity()))
              .apply(
                  "CheckSameKey",
                  CheckSameKey.newBuilder().setErrorTag(errorTag()).setGoodTag(goodTag).build());

      entities
          .get(goodTag)
          .apply("WriteToDatastore", DatastoreIO.v1().write().withProjectId(projectId()));
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

    /** Builder for DatastoreDeleteEntityJson. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setProjectId(ValueProvider<String> projectId);

      public abstract DatastoreDeleteEntityJson build();
    }

    public static Builder newBuilder() {
      return new AutoValue_DatastoreConverters_DatastoreDeleteEntityJson.Builder();
    }

    @Override
    public PDone expand(PCollection<String> entityJson) {
      return entityJson
          .apply("StringToKey", ParDo.of(new JsonToKey()))
          .apply("DeleteKeys", DatastoreIO.v1().deleteKey().withProjectId(projectId()));
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

    /** Processes Datstore entity into json. */
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
    private JsonFormat.Parser jsonParser;

    public EntityJsonParser() {
      TypeRegistry typeRegistry = TypeRegistry.newBuilder().add(Entity.getDescriptor()).build();

      jsonParser = JsonFormat.parser().usingTypeRegistry(typeRegistry);
    }

    public void merge(String json, Entity.Builder entityBuilder)
        throws InvalidProtocolBufferException {
      jsonParser.merge(json, entityBuilder);
    }

    public Entity parse(String json) throws InvalidProtocolBufferException {
      Entity.Builder entityBuilter = Entity.newBuilder();
      merge(json, entityBuilter);
      return entityBuilter.build();
    }
  }

  /** Writes Entities to Datastore. */
  @AutoValue
  public abstract static class WriteEntities
      extends PTransform<PCollection<Entity>, PCollectionTuple> {
    public abstract ValueProvider<String> projectId();

    public abstract TupleTag<String> errorTag();

    /** Builder for WriteJsonEntities. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setProjectId(ValueProvider<String> projectId);

      public abstract Builder setErrorTag(TupleTag<String> errorTag);

      public abstract WriteEntities build();
    }

    public static Builder newBuilder() {
      return new AutoValue_DatastoreConverters_WriteEntities.Builder();
    }

    @Override
    public PCollectionTuple expand(PCollection<Entity> entity) {
      TupleTag<Entity> goodTag = new TupleTag<>();

      // Due to the fact that DatastoreIO does non-transactional writing to Datastore, writing the
      // same entity more than once in the same commit is not supported (error "A non-transactional
      // commit may not contain multiple mutations affecting the same entity). Messages with the
      // same key are thus not written to Datastore and instead routed to an error PCollection for
      // further handlig downstream.
      PCollectionTuple entities =
          entity.apply(
              "CheckSameKey",
              CheckSameKey.newBuilder().setErrorTag(errorTag()).setGoodTag(goodTag).build());
      entities
          .get(goodTag)
          .apply("WriteToDatastore", DatastoreIO.v1().write().withProjectId(projectId()));
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
