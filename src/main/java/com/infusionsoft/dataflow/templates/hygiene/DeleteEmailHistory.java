package com.infusionsoft.dataflow.templates.hygiene;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubReadOptions;
import com.google.datastore.v1.Key;
import com.infusionsoft.dataflow.shared.EntityToKey;
import com.infusionsoft.dataflow.utils.CloudStorageUtils;
import com.infusionsoft.dataflow.utils.DatastoreUtils;
import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.lang3.StringUtils;
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

/**
 * A template that deletes email history by accountId.
 *
 * Used by email-history-api
 *
 * Deploy to sand:
 * mvn compile exec:java -Dexec.mainClass=com.infusionsoft.dataflow.templates.hygiene.DeleteEmailHistory -Dexec.args="--project=is-email-history-api-sand --stagingLocation=gs://dataflow-is-email-history-api-sand/staging --templateLocation=gs://dataflow-is-email-history-api-sand/templates/delete_emails --runner=DataflowRunner --serviceAccount=is-email-history-api-sand@appspot.gserviceaccount.com --datastoreProjectId=is-email-history-api-sand --cloudStorageProjectId=is-email-history-api-sand --cloudStorageBucketName=is-email-history-api-sand.appspot.com"
 *
 * Deploy to prod:
 * mvn compile exec:java -Dexec.mainClass=com.infusionsoft.dataflow.templates.hygiene.DeleteEmailHistory -Dexec.args="--project=is-email-history-api-prod --stagingLocation=gs://dataflow-is-email-history-api-prod/staging --templateLocation=gs://dataflow-is-email-history-api-prod/templates/delete_emails --runner=DataflowRunner --serviceAccount=is-email-history-api-prod@appspot.gserviceaccount.com --datastoreProjectId=is-email-history-api-prod --cloudStorageProjectId=is-email-history-api-prod --cloudStorageBucketName=is-email-history-api-prod.appspot.com"
 *
 */
public class DeleteEmailHistory {

  public interface Options extends PipelineOptions, StreamingOptions, PubsubReadOptions {

    @Description("GCP Project Id of where the datastore entities live")
    ValueProvider<String> getDatastoreProjectId();
    void setDatastoreProjectId(ValueProvider<String> datastoreProjectId);

    @Description("GCP Project Id of where the cloud storage files live")
    ValueProvider<String> getCloudStorageProjectId();
    void setCloudStorageProjectId(ValueProvider<String> cloudStorageProjectId);

    @Description("Bucket Name of where cloud storage files live")
    ValueProvider<String> getCloudStorageBucketName();
    void setCloudStorageBucketName(ValueProvider<String> cloudStorageBucketName);

    @Description("The Account Id whose emails are being deleted")
    ValueProvider<String> getAccountId();
    void setAccountId(ValueProvider<String> accountId);

  }

  public static class DeleteContentFn extends DoFn<Key, Key> {

    private final String projectId;
    private final String bucket;

    public DeleteContentFn(String projectId, String bucket) {
      checkArgument(StringUtils.isNotBlank(projectId), "projectId must not be blank");
      checkArgument(StringUtils.isNotBlank(bucket), "bucket must not be blank");

      this.projectId = projectId;
      this.bucket = bucket;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      final Key key = context.element();
      final long id = DatastoreUtils.getId(key);

      final Storage storage = CloudStorageUtils.getStorage(projectId);

      CloudStorageUtils.delete(storage, bucket, id + ".json", id + ".html", id + ".txt");
      context.output(key);
    }
  }

  public static void main(String[] args) {
    final Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    final String datastoreProjectId = options.getDatastoreProjectId().get();
    final String cloudStorageProjectId = options.getCloudStorageProjectId().get();
    final String cloudStorageBucketName = options.getCloudStorageBucketName().get();

    final Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Find Emails", DatastoreIO.v1().read()
            .withProjectId(datastoreProjectId)
            .withLiteralGqlQuery(NestedValueProvider.of(options.getAccountId(),
                (SerializableFunction<String, String>) accountId -> String.format("SELECT __key__ FROM Email WHERE accountId = '%s'", accountId))))
        .apply("Shard", Reshuffle.viaRandomKey()) // this ensures that the subsequent steps occur in parallel
        .apply("Entity To Key", ParDo.of(new EntityToKey()))
        .apply("Delete from Cloud Storage", ParDo.of(new DeleteContentFn(cloudStorageProjectId, cloudStorageBucketName)))
        .apply("Delete from Datastore", DatastoreIO.v1().deleteKey()
            .withProjectId(datastoreProjectId));

    pipeline.run();
  }
}