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

import static com.google.common.base.Preconditions.checkArgument;
import static com.infusionsoft.dataflow.utils.JavaTimeUtils.UTC;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubReadOptions;
import com.google.common.base.Charsets;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.infusionsoft.dataflow.dto.ColdEmail;
import com.infusionsoft.dataflow.dto.EmailContent;
import com.infusionsoft.dataflow.shared.DeleteEmailContent;
import com.infusionsoft.dataflow.transformers.ColdEmailTransformer;
import com.infusionsoft.dataflow.utils.CloudStorageUtils;
import com.infusionsoft.dataflow.utils.DatastoreUtils;
import com.infusionsoft.dataflow.utils.JavaTimeUtils;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.entity.ContentType;

/**
 * A template that archives email history.
 *
 * <p>Used by email-history-api
 *
 * <p>Deploy to sand: mvn compile exec:java
 * -Dexec.mainClass=com.infusionsoft.dataflow.templates.hygiene.ArchiveEmailHistory
 * -Dexec.args="--project=is-email-history-api-sand
 * --stagingLocation=gs://dataflow-is-email-history-api-sand/staging
 * --templateLocation=gs://dataflow-is-email-history-api-sand/templates/archive_emails
 * --runner=DataflowRunner --serviceAccount=is-email-history-api-sand@appspot.gserviceaccount.com
 * --datastoreProjectId=is-email-history-api-sand --cloudStorageProjectId=is-email-history-api-sand
 * --warmCloudStorageBucketName=is-email-history-api-sand.appspot.com
 * --coldCloudStorageBucketName=archive-is-email-history-api-sand.appspot.com"
 *
 * <p>Deploy to prod: mvn compile exec:java
 * -Dexec.mainClass=com.infusionsoft.dataflow.templates.hygiene.ArchiveEmailHistory
 * -Dexec.args="--project=is-email-history-api-prod
 * --stagingLocation=gs://dataflow-is-email-history-api-prod/staging
 * --templateLocation=gs://dataflow-is-email-history-api-prod/templates/archive_emails
 * --runner=DataflowRunner --serviceAccount=is-email-history-api-prod@appspot.gserviceaccount.com
 * --datastoreProjectId=is-email-history-api-prod --cloudStorageProjectId=is-email-history-api-prod
 * --warmCloudStorageBucketName=is-email-history-api-prod.appspot.com
 * --coldCloudStorageBucketName=archive-is-email-history-api-prod.appspot.com"
 */
public class ArchiveEmailHistory {

  public interface Options extends PipelineOptions, StreamingOptions, PubsubReadOptions {

    @Description("GCP Project Id of where the datastore entities live")
    ValueProvider<String> getDatastoreProjectId();

    void setDatastoreProjectId(ValueProvider<String> datastoreProjectId);

    @Description("GCP Project Id of where the cloud storage files live")
    ValueProvider<String> getCloudStorageProjectId();

    void setCloudStorageProjectId(ValueProvider<String> cloudStorageProjectId);

    @Description("Bucket Name of where warm cloud storage files live")
    ValueProvider<String> getWarmCloudStorageBucketName();

    void setWarmCloudStorageBucketName(ValueProvider<String> warmCloudStorageBucketName);

    @Description("Bucket Name of where cold cloud storage files live")
    ValueProvider<String> getColdCloudStorageBucketName();

    void setColdCloudStorageBucketName(ValueProvider<String> coldCloudStorageBucketName);

    @Description("How many months until an email expires")
    ValueProvider<Integer> getExpireMonths();

    void setExpireMonths(ValueProvider<Integer> expireMonths);
  }

  public static class ArchiveEmailFn extends DoFn<Entity, Key> {

    private static final ObjectMapper OBJECT_MAPPER =
        new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private final String projectId;
    private final String warmBucket;
    private final String coldBucket;

    public ArchiveEmailFn(String projectId, String warmBucket, String coldBucket) {
      checkArgument(StringUtils.isNotBlank(projectId), "projectId must not be blank");
      checkArgument(StringUtils.isNotBlank(warmBucket), "warmBucket must not be blank");
      checkArgument(StringUtils.isNotBlank(coldBucket), "coldBucket must not be blank");

      this.projectId = projectId;
      this.warmBucket = warmBucket;
      this.coldBucket = coldBucket;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      final Entity entity = context.element();
      final com.google.datastore.v1.Key key = entity.getKey();
      final long id = DatastoreUtils.getId(key);

      final Storage storage = CloudStorageUtils.getStorage(projectId);
      final List<Blob> blobs =
          CloudStorageUtils.load(storage, warmBucket, id + ".json", id + ".html", id + ".txt");

      String htmlBody = null;
      String textBody = null;

      for (Blob blob : blobs) {
        final ContentType contentType = ContentType.getByMimeType(blobs.get(0).getContentType());

        if (contentType == ContentType.APPLICATION_JSON) {
          final String json = new String(blob.getContent(), Charsets.UTF_8);
          final EmailContent content = OBJECT_MAPPER.readValue(json, EmailContent.class);

          htmlBody = content.getHtmlBody();
          textBody = content.getTextBody();

        } else if (contentType == ContentType.TEXT_HTML) {
          htmlBody = new String(blob.getContent(), Charsets.UTF_8);

        } else if (contentType == ContentType.TEXT_PLAIN) {
          textBody = new String(blob.getContent(), Charsets.UTF_8);
        }
      }

      final ColdEmail dto = ColdEmailTransformer.fromEntity(entity, htmlBody, textBody);
      final String accountId = dto.getAccountId();
      final ZonedDateTime created = dto.getCreated();

      final String fileName =
          String.format(
              "%s_%d-%d-%d_%d.json",
              accountId, created.getYear(), created.getMonthValue(), created.getDayOfMonth(), id);

      CloudStorageUtils.upload(storage, coldBucket, fileName, null, ContentType.APPLICATION_JSON);
      context.output(key);
    }
  }

  public static void main(String[] args) {
    final Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    final String datastoreProjectId = options.getDatastoreProjectId().get();
    final String cloudStorageProjectId = options.getCloudStorageProjectId().get();
    final String warmCloudStorageBucket = options.getWarmCloudStorageBucketName().get();
    final String coldCloudStorageBucket = options.getColdCloudStorageBucketName().get();

    final Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(
            "Find Emails",
            DatastoreIO.v1()
                .read()
                .withProjectId(datastoreProjectId)
                .withLiteralGqlQuery(
                    NestedValueProvider.of(
                        options.getExpireMonths(),
                        (SerializableFunction<Integer, String>)
                            expireMonths -> {
                              final ZonedDateTime expired =
                                  ZonedDateTime.now(UTC).minusMonths(expireMonths);

                              return String.format(
                                  "SELECT * FROM Email WHERE created < %s",
                                  JavaTimeUtils.formatForGql(expired));
                            })))
        .apply(
            "Shard",
            Reshuffle.viaRandomKey()) // this ensures that the subsequent steps occur in parallel
        .apply(
            "Upload to Cold Storage",
            ParDo.of(
                new ArchiveEmailFn(
                    cloudStorageProjectId, warmCloudStorageBucket, coldCloudStorageBucket)))
        .apply(
            "Delete from Warm Storage",
            ParDo.of(new DeleteEmailContent(cloudStorageProjectId, warmCloudStorageBucket)))
        .apply(
            "Delete from Datastore",
            DatastoreIO.v1().deleteKey().withProjectId(datastoreProjectId));

    pipeline.run();
  }
}
