package com.infusionsoft.dataflow.templates.hygiene;

import static com.infusionsoft.dataflow.utils.JavaTimeUtils.UTC;

import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubReadOptions;
import com.infusionsoft.dataflow.shared.EntityToKey;
import com.infusionsoft.dataflow.utils.JavaTimeUtils;
import java.time.ZonedDateTime;
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
 * A template that deletes expired hosted emails.
 *
 * Used by hosted-email-api
 *
 * Deploy to sand:
 * mvn compile exec:java -Dexec.mainClass=com.infusionsoft.dataflow.templates.hygiene.DeleteExpiredHostedEmails -Dexec.args="--project=is-hosted-email-api-sand --stagingLocation=gs://dataflow-is-hosted-email-api-sand/staging --templateLocation=gs://dataflow-is-hosted-email-api-sand/templates/delete_expired_emails --runner=DataflowRunner --serviceAccount=is-hosted-email-api-sand@appspot.gserviceaccount.com --datastoreProjectId=is-hosted-email-api-sand"
 *
 * Deploy to prod:
 * mvn compile exec:java -Dexec.mainClass=com.infusionsoft.dataflow.templates.hygiene.DeleteExpiredHostedEmails -Dexec.args="--project=is-hosted-email-api-prod --stagingLocation=gs://dataflow-is-hosted-email-api-prod/staging --templateLocation=gs://dataflow-is-hosted-email-api-prod/templates/delete_expired_emails --runner=DataflowRunner --serviceAccount=is-hosted-email-api-prod@appspot.gserviceaccount.com --datastoreProjectId=is-hosted-email-api-prod"
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

  public static void main(String[] args) {
    final Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    final String projectId = options.getDatastoreProjectId().get();

    final Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Find Emails", DatastoreIO.v1().read()
            .withProjectId(projectId)
            .withLiteralGqlQuery(NestedValueProvider.of(options.getExpireDays(), (SerializableFunction<Integer, String>) expireDays -> {
              final ZonedDateTime expired = ZonedDateTime.now(UTC)
                  .minusDays(expireDays);

              return String.format("SELECT __key__ FROM Email WHERE created < %s", JavaTimeUtils.formatForGql(expired));
            })))
        .apply("Shard", Reshuffle.viaRandomKey()) // this ensures that the subsequent steps occur in parallel
        .apply("Entity To Key", ParDo.of(new EntityToKey()))
        .apply("Delete By Key", DatastoreIO.v1().deleteKey()
            .withProjectId(projectId));

    pipeline.run();
  }
}