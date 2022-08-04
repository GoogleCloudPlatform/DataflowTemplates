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
import com.infusionsoft.dataflow.shared.EntityToKey;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * A template that deletes hosted emails by accountId.
 *
 * <p>Used by hosted-email-api
 *
 * <p>Deploy to sand: mvn compile exec:java
 * -Dexec.mainClass=com.infusionsoft.dataflow.templates.hygiene.DeleteHostedEmails
 * -Dexec.args="--project=is-hosted-email-api-sand
 * --stagingLocation=gs://dataflow-is-hosted-email-api-sand/staging
 * --templateLocation=gs://dataflow-is-hosted-email-api-sand/templates/delete_emails
 * --runner=DataflowRunner --serviceAccount=is-hosted-email-api-sand@appspot.gserviceaccount.com
 * --datastoreProjectId=is-hosted-email-api-sand"
 *
 * <p>Deploy to prod: mvn compile exec:java
 * -Dexec.mainClass=com.infusionsoft.dataflow.templates.hygiene.DeleteHostedEmails
 * -Dexec.args="--project=is-hosted-email-api-prod
 * --stagingLocation=gs://dataflow-is-hosted-email-api-prod/staging
 * --templateLocation=gs://dataflow-is-hosted-email-api-prod/templates/delete_emails
 * --runner=DataflowRunner --serviceAccount=is-hosted-email-api-prod@appspot.gserviceaccount.com
 * --datastoreProjectId=is-hosted-email-api-prod"
 */
public class DeleteHostedEmails {

  public interface Options extends PipelineOptions, StreamingOptions, PubsubReadOptions {

    @Description("GCP Project Id of where the datastore entities live")
    ValueProvider<String> getDatastoreProjectId();

    void setDatastoreProjectId(ValueProvider<String> datastoreProjectId);

    @Description("The Account Id whose emails are being deleted")
    ValueProvider<String> getAccountId();

    void setAccountId(ValueProvider<String> accountId);
  }

  public static void main(String[] args) {
    final Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    final String projectId = options.getDatastoreProjectId().get();

    final Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(
            "Find Emails",
            DatastoreIO.v1()
                .read()
                .withProjectId(projectId)
                .withLiteralGqlQuery(
                    NestedValueProvider.of(
                        options.getAccountId(),
                        (SerializableFunction<String, String>)
                            accountId ->
                                String.format(
                                    "SELECT __key__ FROM Email WHERE accountUid = '%s'",
                                    accountId))))
        .apply(
            "Shard",
            Reshuffle.viaRandomKey()) // this ensures that the subsequent steps occur in parallel
        .apply("Entity To Key", ParDo.of(new EntityToKey()))
        .apply("Delete By Key", DatastoreIO.v1().deleteKey().withProjectId(projectId));

    pipeline.run();
  }
}
