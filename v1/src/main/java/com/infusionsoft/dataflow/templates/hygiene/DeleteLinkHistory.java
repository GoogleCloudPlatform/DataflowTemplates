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
package com.infusionsoft.dataflow.templates.hygiene;

import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubReadOptions;
import com.google.datastore.v1.Entity;
import com.infusionsoft.dataflow.shared.EntityToKey;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

/**
 * A template that deletes link history by accountId.
 *
 * <p>Used by tracking-link-api
 *
 * <p>Deploy to sand: mvn compile exec:java
 * -Dexec.mainClass=com.infusionsoft.dataflow.templates.hygiene.DeleteLinkHistory
 * -Dexec.args="--project=is-tracking-link-api-sand
 * --stagingLocation=gs://dataflow-is-tracking-link-api-sand/staging
 * --templateLocation=gs://dataflow-is-tracking-link-api-sand/templates/delete_links
 * --runner=DataflowRunner --serviceAccount=is-tracking-link-api-sand@appspot.gserviceaccount.com
 * --datastoreProjectId=is-tracking-link-api-sand"
 *
 * <p>Deploy to prod: mvn compile exec:java
 * -Dexec.mainClass=com.infusionsoft.dataflow.templates.hygiene.DeleteLinkHistory
 * -Dexec.args="--project=is-tracking-link-api-prod
 * --stagingLocation=gs://dataflow-is-tracking-link-api-prod/staging
 * --templateLocation=gs://dataflow-is-tracking-link-api-prod/templates/delete_links
 * --runner=DataflowRunner --serviceAccount=is-tracking-link-api-prod@appspot.gserviceaccount.com
 * --datastoreProjectId=is-tracking-link-api-prod"
 */
public class DeleteLinkHistory {

  public interface Options extends PipelineOptions, StreamingOptions, PubsubReadOptions {

    @Description("GCP Project Id of where the datastore entities live")
    ValueProvider<String> getDatastoreProjectId();

    void setDatastoreProjectId(ValueProvider<String> datastoreProjectId);

    @Description("The Account Id whose links are being deleted")
    ValueProvider<String> getAccountId();

    void setAccountId(ValueProvider<String> accountId);
  }

  public static void main(String[] args) {
    final Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    final String projectId = options.getDatastoreProjectId().get();

    final Pipeline pipeline = Pipeline.create(options);

    final PCollection<Entity> links =
        pipeline.apply(
            "Find Links",
            DatastoreIO.v1()
                .read()
                .withProjectId(projectId)
                .withLiteralGqlQuery(
                    NestedValueProvider.of(
                        options.getAccountId(),
                        (SerializableFunction<String, String>)
                            accountId ->
                                String.format(
                                    "SELECT __key__ FROM Link WHERE accountUid = '%s'",
                                    accountId))));

    final PCollection<Entity> linkGroups =
        pipeline.apply(
            "Find Link Groups",
            DatastoreIO.v1()
                .read()
                .withProjectId(projectId)
                .withLiteralGqlQuery(
                    NestedValueProvider.of(
                        options.getAccountId(),
                        (SerializableFunction<String, String>)
                            accountId ->
                                String.format(
                                    "SELECT __key__ FROM LinkGroup WHERE accountUid = '%s'",
                                    accountId))));

    final PCollection<Entity> clicks =
        pipeline.apply(
            "Find Clicks",
            DatastoreIO.v1()
                .read()
                .withProjectId(projectId)
                .withLiteralGqlQuery(
                    NestedValueProvider.of(
                        options.getAccountId(),
                        (SerializableFunction<String, String>)
                            accountId ->
                                String.format(
                                    "SELECT __key__ FROM Click WHERE accountUid = '%s'",
                                    accountId))));

    final PCollection<Entity> uniqueClicks =
        pipeline.apply(
            "Find UniqueClicks",
            DatastoreIO.v1()
                .read()
                .withProjectId(projectId)
                .withLiteralGqlQuery(
                    NestedValueProvider.of(
                        options.getAccountId(),
                        (SerializableFunction<String, String>)
                            accountId ->
                                String.format(
                                    "SELECT __key__ FROM UniqueClick WHERE accountUid = '%s'",
                                    accountId))));

    final PCollection<Entity> groupedClicks =
        pipeline.apply(
            "Find GroupedClicks",
            DatastoreIO.v1()
                .read()
                .withProjectId(projectId)
                .withLiteralGqlQuery(
                    NestedValueProvider.of(
                        options.getAccountId(),
                        (SerializableFunction<String, String>)
                            accountId ->
                                String.format(
                                    "SELECT __key__ FROM GroupedClick WHERE accountUid = '%s'",
                                    accountId))));

    final PCollection<Entity> contacts =
        pipeline.apply(
            "Find Contacts",
            DatastoreIO.v1()
                .read()
                .withProjectId(projectId)
                .withLiteralGqlQuery(
                    NestedValueProvider.of(
                        options.getAccountId(),
                        (SerializableFunction<String, String>)
                            accountId ->
                                String.format(
                                    "SELECT __key__ FROM Contact WHERE accountUid = '%s'",
                                    accountId))));

    final PCollection<Entity> uniqueContacts =
        pipeline.apply(
            "Find UniqueContacts",
            DatastoreIO.v1()
                .read()
                .withProjectId(projectId)
                .withLiteralGqlQuery(
                    NestedValueProvider.of(
                        options.getAccountId(),
                        (SerializableFunction<String, String>)
                            accountId ->
                                String.format(
                                    "SELECT __key__ FROM UniqueContact WHERE accountUid = '%s'",
                                    accountId))));

    final PCollection<Entity> composite =
        PCollectionList.of(links)
            .and(linkGroups)
            .and(clicks)
            .and(uniqueClicks)
            .and(groupedClicks)
            .and(contacts)
            .and(uniqueContacts)
            .apply(Flatten.pCollections());

    composite
        .apply(
            "Shard",
            Reshuffle.viaRandomKey()) // this ensures that the subsequent steps occur in parallel
        .apply("Entity To Key", ParDo.of(new EntityToKey()))
        .apply("Delete By Key", DatastoreIO.v1().deleteKey().withProjectId(projectId));

    pipeline.run();
  }
}
