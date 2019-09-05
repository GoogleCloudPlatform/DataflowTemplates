package com.infusionsoft.dataflow.templates;

import static com.infusionsoft.dataflow.utils.ZonedDateTimeUtils.UTC;

import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubReadOptions;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.infusionsoft.dataflow.utils.ZonedDateTimeUtils;
import java.time.ZonedDateTime;
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
 * A template that deletes expired hosted emails.
 *
 * Used by hosted-email-api
 *
 * Deploy to sand:
 * mvn compile exec:java -Dexec.mainClass=com.infusionsoft.dataflow.templates.DeleteExpiredHostedEmails -Dexec.args="--project=is-hosted-email-api-sand --stagingLocation=gs://dataflow-is-hosted-email-api-sand/staging --templateLocation=gs://dataflow-is-hosted-email-api-sand/templates/delete_expired_emails --runner=DataflowRunner --serviceAccount=is-hosted-email-api-sand@appspot.gserviceaccount.com --datastoreProjectId=is-hosted-email-api-sand"
 *
 * Deploy to prod:
 * mvn compile exec:java -Dexec.mainClass=com.infusionsoft.dataflow.templates.DeleteExpiredHostedEmails -Dexec.args="--project=is-hosted-email-api-prod --stagingLocation=gs://dataflow-is-hosted-email-api-prod/staging --templateLocation=gs://dataflow-is-hosted-email-api-prod/templates/delete_expired_emails --runner=DataflowRunner --serviceAccount=is-hosted-email-api-prod@appspot.gserviceaccount.com --datastoreProjectId=is-hosted-email-api-prod"
 *
 */
public class DeleteExpiredHostedEmails {

  public interface Options extends PipelineOptions, StreamingOptions, PubsubReadOptions {

    @Description("GCP Project Id of where the datastore entities live")
    ValueProvider<String> getDatastoreProjectId();
    void setDatastoreProjectId(ValueProvider<String> datastoreProjectId);

    @Description("How many days until an email expires")
    ValueProvider<Integer> getExpireDays();
    void setExpireDays(ValueProvider<Integer> expireDays);

  }

  public static class EntityToKey extends DoFn<Entity, Key> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      final Entity entity = context.element();

      context.output(entity.getKey());
    }
  }

  public static void main(String[] args) {
    final Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    final String projectId = options.getDatastoreProjectId().get();

    final Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Find Expired Emails", DatastoreIO.v1().read()
            .withProjectId(projectId)
            .withLiteralGqlQuery(NestedValueProvider.of(options.getExpireDays(), (SerializableFunction<Integer, String>) expireDays -> {
              final ZonedDateTime expired = ZonedDateTime.now(UTC)
                  .minusDays(expireDays);

              return String.format("SELECT __key__ FROM Email WHERE created < %s", ZonedDateTimeUtils.formatForGql(expired));
            })))
        .apply("Shard", Reshuffle.viaRandomKey()) // this ensures that the subsequent steps occur in parallel
        .apply("Entity To Key", ParDo.of(new EntityToKey()))
        .apply("Delete By Key", DatastoreIO.v1().deleteKey()
            .withProjectId(projectId));

    pipeline.run();
  }
}