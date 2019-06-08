package com.infusionsoft.dataflow.templates.migration;

import static com.google.common.base.Preconditions.checkArgument;

import com.infusionsoft.dataflow.utils.CloudStorageUtils;
import com.infusionsoft.dataflow.utils.DatastoreUtils;

import com.google.cloud.storage.Storage;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A template that moves email content to cloud storage (because that is 10x cheaper than using Datastore)
 *
 * Used by email-history-api
 *
 * Deploy to sand:
 * mvn compile exec:java -Dexec.mainClass=com.infusionsoft.dataflow.templates.migration.EmailContent -Dexec.args="--project=is-email-history-api-sand --stagingLocation=gs://dataflow-is-email-history-api-sand/staging --templateLocation=gs://dataflow-is-email-history-api-sand/templates/migration_content --runner=DataflowRunner --serviceAccount=is-email-history-api-sand@appspot.gserviceaccount.com --datastoreProjectId=is-email-history-api-sand --cloudStorageProjectId=is-email-history-api-sand --cloudStorageBucketName=is-email-history-api-sand.appspot.com"
 *
 * n1-highcpu-64
 *
 * Deploy to prod:
 * mvn compile exec:java -Dexec.mainClass=com.infusionsoft.dataflow.templates.migration.EmailContent -Dexec.args="--project=is-email-history-api-prod --stagingLocation=gs://dataflow-is-email-history-api-prod/staging --templateLocation=gs://dataflow-is-email-history-api-prod/templates/migration_content --runner=DataflowRunner --serviceAccount=is-email-history-api-prod@appspot.gserviceaccount.com --datastoreProjectId=is-email-history-api-prod --cloudStorageProjectId=is-email-history-api-prod --cloudStorageBucketName=is-email-history-api-prod.appspot.com"
 *
 * n1-highcpu-96
 *
 */
public class EmailContent {

  public interface Options extends PipelineOptions {

    @Description("GCP Project Id of where the datastore entities live")
    ValueProvider<String> getDatastoreProjectId();
    void setDatastoreProjectId(ValueProvider<String> datastoreProjectId);

    @Description("GCP Project Id of where the cloud storage files live")
    ValueProvider<String> getCloudStorageProjectId();
    void setCloudStorageProjectId(ValueProvider<String> cloudStorageProjectId);

    @Description("Bucket Name of where cloud storage files live")
    ValueProvider<String> getCloudStorageBucketName();
    void setCloudStorageBucketName(ValueProvider<String> cloudStorageBucketName);

  }

  public static class SaveContentFn extends DoFn<Entity, Entity> {

    private static final Logger LOG = LoggerFactory.getLogger(RemoveContentFn.class);

    private final String projectId;
    private final String bucket;

    public SaveContentFn(String projectId, String bucket) {
      checkArgument(StringUtils.isNotBlank(projectId), "projectId must not be blank");
      checkArgument(StringUtils.isNotBlank(bucket), "bucket must not be blank");

      this.projectId = projectId;
      this.bucket = bucket;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      final Entity entity = context.element();
      final long id = DatastoreUtils.getId(entity.getKey());
      final Map<String, Value> properties = entity.getProperties();

      if (properties.containsKey("htmlBody") && properties.containsKey("textBody")) {
        final String htmlBody = properties.get("htmlBody").getStringValue();
        final String textBody = properties.get("textBody").getStringValue();

        final Storage storage = CloudStorageUtils.getStorage(projectId);

        if (StringUtils.isNotBlank(htmlBody)) {
          final String fileName = id + ".html";
          LOG.debug(fileName);

          CloudStorageUtils.upload(storage, bucket, fileName, htmlBody, ContentType.TEXT_HTML);
        }

        if (StringUtils.isNotBlank(textBody)) {
          final String fileName = id + ".txt";
          LOG.debug(fileName);

          CloudStorageUtils.upload(storage, bucket, fileName, textBody, ContentType.TEXT_PLAIN);
        }

        context.output(entity);
      }
    }
  }

  public static class RemoveContentFn extends DoFn<Entity, Entity> {

    private static final Logger LOG = LoggerFactory.getLogger(RemoveContentFn.class);

    @ProcessElement
    public void processElement(ProcessContext context) {
      final Entity original = context.element();

      final Entity modified = original.toBuilder()
          .removeProperties("htmlBody")
          .removeProperties("textBody")
          .build();

      LOG.debug("migrated: {} -> {}", original, modified);
      context.output(modified);
    }
  }

  public static void main(String[] args) {
    final Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    final String datastoreProjectId = options.getDatastoreProjectId().get();
    final String cloudStorageProjectId = options.getCloudStorageProjectId().get();
    final String cloudStorageBucketName = options.getCloudStorageBucketName().get();

    final Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Load Emails", DatastoreIO.v1().read()
            .withProjectId(datastoreProjectId)
            .withLiteralGqlQuery("SELECT * FROM Email"))
        .apply("Upload Content To Cloud Storage", ParDo.of(new SaveContentFn(cloudStorageProjectId, cloudStorageBucketName)))
        .apply("Remove Content From Datastore", ParDo.of(new RemoveContentFn()))
        .apply("Save Emails", DatastoreIO.v1().write()
            .withProjectId(datastoreProjectId));

    pipeline.run();
  }
}