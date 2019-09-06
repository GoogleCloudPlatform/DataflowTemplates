package com.infusionsoft.dataflow.templates;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubReadOptions;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.GqlQuery;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreException;
import com.google.datastore.v1.client.DatastoreHelper;
import com.infusionsoft.dataflow.utils.DatastoreUtils;
import com.infusionsoft.dataflow.utils.JavaTimeUtils;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A template that listens to pubsub and marks emails as opened, as appropriate.
 *
 * Used by email-history-api
 *
 * Deploy to sand:
 * mvn compile exec:java -Dexec.mainClass=com.infusionsoft.dataflow.templates.PubsubMarkEmailsOpened -Dexec.args="--project=is-email-history-api-sand --stagingLocation=gs://dataflow-is-email-history-api-sand/staging --templateLocation=gs://dataflow-is-email-history-api-sand/templates/ps_mark_email_opened --runner=DataflowRunner --serviceAccount=is-email-history-api-sand@appspot.gserviceaccount.com --datastoreProjectId=is-email-history-api-sand"
 *
 * projects/is-tracking-pixel-api-sand/topics/v1.render
 *
 * Deploy to prod:
 * mvn compile exec:java -Dexec.mainClass=com.infusionsoft.dataflow.templates.PubsubMarkEmailsOpened -Dexec.args="--project=is-email-history-api-prod --stagingLocation=gs://dataflow-is-email-history-api-prod/staging --templateLocation=gs://dataflow-is-email-history-api-prod/templates/ps_mark_email_opened --runner=DataflowRunner --serviceAccount=is-email-history-api-prod@appspot.gserviceaccount.com --datastoreProjectId=is-email-history-api-prod"
 *
 * projects/is-tracking-pixel-api-prod/topics/v1.render
 *
 */
public class PubsubMarkEmailsOpened {

  /**
   * Options supported by {@link PubsubMarkEmailsOpened}.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, StreamingOptions,
      PubsubReadOptions {

    @Description("GCP Project Id of where the datastore entities live")
    ValueProvider<String> getDatastoreProjectId();
    void setDatastoreProjectId(ValueProvider<String> datastoreProjectId);

  }

  public static class ExtractAndHandleEventsFn extends DoFn<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(ExtractAndHandleEventsFn.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private String projectId;

    @StartBundle
    public void startBundle(StartBundleContext context) {
      if (this.projectId != null) {
        return; // has been evaluated already
      }

      final Options options = context.getPipelineOptions().as(Options.class);
      projectId = (options.getDatastoreProjectId() == null ? null : options.getDatastoreProjectId().get());
      LOG.info("Enabling event filter [projectId: {}]", projectId);
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      final String message = context.element();
      LOG.debug("processing... {}", message);

      final Map<String, Object> json = OBJECT_MAPPER.readValue(message, Map.class);

      processEvent(context, json);
    }

    private void processEvent(ProcessContext context, Map<String, Object> json) {
      final String accountId = (String) json.get("accountId");
      final String pixelId = (String) json.get("pixelId");
      final String contactId = (String) json.get("contactId");
      final ZonedDateTime timestamp = ZonedDateTime.parse((String) json.get("timestamp"));

      final Datastore datastore = DatastoreUtils.getDatastore(context.getPipelineOptions(), projectId);

      findEmails(datastore, accountId, pixelId, contactId).stream()
          .forEach(entity -> markOpened(datastore, entity, timestamp));
    }

    private void markOpened(Datastore datastore, Entity entity, ZonedDateTime timestamp) {
      final Entity updated = entity.toBuilder()
          .putProperties("opened", Value.newBuilder()
              .setTimestampValue(JavaTimeUtils.toTimestamp(timestamp))
              .build())
          .build();

      final CommitRequest request = CommitRequest.newBuilder()
          .addMutations(DatastoreHelper.makeUpdate(updated))
          .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
          .build();

      try {
        datastore.commit(request);
      } catch (DatastoreException e) {
        LOG.error("Unable to mark opened: " + entity, e);
      }
    }

    private List<Entity> findEmails(Datastore datastore,
                                    String accountId, String pixelId, String contactId) {

      final List<Entity> entities = new LinkedList<>();

      final StringBuilder gql = new StringBuilder("SELECT * FROM Email")
          .append(" WHERE ").append(String.format("accountId = '%s'", accountId))
          .append(" AND ").append(String.format("pixelId = '%s'", pixelId))
          .append(" AND ").append(String.format("contactId = '%s'", contactId));

      LOG.debug(gql.toString());

      final RunQueryRequest request = RunQueryRequest.newBuilder()
          .setGqlQuery(GqlQuery.newBuilder()
              .setQueryString(gql.toString())
              .setAllowLiterals(true)
              .build())
          .build();

      try {
        final RunQueryResponse response = datastore.runQuery(request);
        final QueryResultBatch batch = response.getBatch();

        batch.getEntityResultsList().stream()
            .map(EntityResult::getEntity)
            .forEach(entities::add);

      } catch (DatastoreException e) {
        LOG.error("Couldn't find emails", e);
      }

      return entities;
    }
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    // Parse the user options passed from the command-line
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    options.setStreaming(true);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Read Events", PubsubIO.readStrings()
            .fromTopic(options.getPubsubReadTopic()))
        .apply("Shard Events", Reshuffle.viaRandomKey())  // this ensures that we handle the events in parallel
        .apply("Handle Events", ParDo.of(new ExtractAndHandleEventsFn()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}