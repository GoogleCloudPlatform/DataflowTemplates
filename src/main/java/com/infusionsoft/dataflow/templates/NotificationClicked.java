package com.infusionsoft.dataflow.templates;

import com.infusionsoft.dataflow.utils.DatastoreUtils;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.GqlQuery;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A template that looks for clicked delinquent email notifications
 *
 * Used by notifications-api
 *
 * Deploy to prod:
 * mvn compile exec:java -Dexec.mainClass=com.infusionsoft.dataflow.templates.NotificationClicked -Dexec.args="--project=is-notifications-api-prod --stagingLocation=gs://dataflow-is-notifications-api-prod/staging --templateLocation=gs://dataflow-is-notifications-api-prod/templates/notification_clicked --runner=DataflowRunner --serviceAccount=is-notifications-api-prod@appspot.gserviceaccount.com --datastoreProjectId=is-notifications-api-prod"
 *
 * gcloud dataflow jobs run notification-clicked --project=is-notifications-api-prod --gcs-location=gs://dataflow-is-notifications-api-prod/templates/notification_clicked --service-account-email=is-notifications-api-prod@appspot.gserviceaccount.com
 *
 */
public class NotificationClicked {

  public interface Options extends PipelineOptions {

    @Description("GCP Project Id of where the datastore entities live")
    ValueProvider<String> getDatastoreProjectId();
    void setDatastoreProjectId(ValueProvider<String> datastoreProjectId);

  }

  public static class CheckFn extends DoFn<Entity, String> {

    private static final Logger LOG = LoggerFactory.getLogger(CheckFn.class);

    private final String projectId;

    public CheckFn(String projectId) {
      this.projectId = projectId;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      final Entity notification = context.element();
      final long id = DatastoreUtils.getId(notification.getKey());
      final String accountId = notification.getPropertiesOrThrow("accountId").getStringValue();

      final Datastore datastore = DatastoreUtils.getDatastore(context.getPipelineOptions(), projectId);

      final RunQueryRequest request = RunQueryRequest.newBuilder()
          .setGqlQuery(GqlQuery.newBuilder()
              .setQueryString(String.format("SELECT * FROM NotificationTracking WHERE __key__ = Key(NotificationTracking, %d)", id))
              .setAllowLiterals(true)
              .build())
          .build();

      try {
        final RunQueryResponse response = datastore.runQuery(request);
        final QueryResultBatch batch = response.getBatch();
        final List<EntityResult> list = batch.getEntityResultsList();

        if (list.size() > 0) {
          final Entity tracking = list.get(0).getEntity();
          final boolean clicked = tracking.getPropertiesOrThrow("emailActionLinkClicked").getBooleanValue();

          if (clicked) {
            context.output(accountId);
          }
        }
      } catch (DatastoreException e) {
        LOG.error("Couldn't find tracking", e);
      }
    }
  }

  public static void main(String[] args) {
    final Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    final String projectId = options.getDatastoreProjectId().get();

    final Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Load", DatastoreIO.v1().read()
            .withProjectId(projectId)
            .withLiteralGqlQuery("SELECT * FROM Notification WHERE source='data' AND subType='delinquentEmail'"))
        .apply("Check", ParDo.of(new CheckFn(projectId)))
        .apply("Log", TextIO.write()
            .to("gs://dataflow-is-notifications-api-prod/output/delinquentEmail_clicked.txt"));

    pipeline.run();
  }
}