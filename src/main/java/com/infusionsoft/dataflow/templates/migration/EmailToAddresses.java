package com.infusionsoft.dataflow.templates.migration;

import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A template that populates the toAddresses field for emails with data in the legacy toAddress field
 *
 * Used by email-history-api
 *
 * Deploy to sand:
 * mvn compile exec:java -Dexec.mainClass=com.infusionsoft.dataflow.templates.migration.EmailToAddresses -Dexec.args="--project=is-email-history-api-sand --stagingLocation=gs://dataflow-is-email-history-api-sand/staging --templateLocation=gs://dataflow-is-email-history-api-sand/templates/migration_to_addresses --runner=DataflowRunner --serviceAccount=is-email-history-api-sand@appspot.gserviceaccount.com --datastoreProjectId=is-email-history-api-sand"
 *
 * n1-highcpu-32
 *
 * Deploy to prod:
 * mvn compile exec:java -Dexec.mainClass=com.infusionsoft.dataflow.templates.migration.EmailToAddresses -Dexec.args="--project=is-email-history-api-prod --stagingLocation=gs://dataflow-is-email-history-api-prod/staging --templateLocation=gs://dataflow-is-email-history-api-prod/templates/migration_to_addresses --runner=DataflowRunner --serviceAccount=is-email-history-api-prod@appspot.gserviceaccount.com --datastoreProjectId=is-email-history-api-prod"
 *
 * n1-highcpu-64
 *
 */
public class EmailToAddresses {

  public interface Options extends PipelineOptions {

    @Description("GCP Project Id of where the datastore entities live")
    ValueProvider<String> getDatastoreProjectId();
    void setDatastoreProjectId(ValueProvider<String> datastoreProjectId);

  }

  public static class MigrateEmailFn extends DoFn<Entity, Entity> {

    private static final Logger LOG = LoggerFactory.getLogger(MigrateEmailFn.class);

    @ProcessElement
    public void processElement(ProcessContext context) {
      final Entity original = context.element();
      final Map<String, Value> properties = original.getProperties();

      if (properties.containsKey("toAddress")) {
        final String toAddress = properties.containsKey("toAddress") ? properties.get("toAddress").getStringValue() : null;
        final Entity.Builder builder = original.toBuilder()
            .removeProperties("toAddress");

        if (StringUtils.isNotBlank(toAddress)) {
          builder
              .putProperties("toAddresses", Value.newBuilder()
                  .setArrayValue(ArrayValue.newBuilder()
                      .addValues(Value.newBuilder()
                          .setStringValue(toAddress)
                          .build())
                      .build())
                  .build())
              .build();
        }

        final Entity modified = builder.build();

        LOG.debug("migrated: {} -> {}", original, modified);
        context.output(modified);
      }
    }
  }

  public static void main(String[] args) {
    final Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    final String projectId = options.getDatastoreProjectId().get();

    final Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Load Emails", DatastoreIO.v1().read()
            .withProjectId(projectId)
            .withLiteralGqlQuery("SELECT * FROM Email"))
        .apply("Do Migration", ParDo.of(new MigrateEmailFn()))
        .apply("Save Emails", DatastoreIO.v1().write()
                .withProjectId(projectId));

    pipeline.run();
  }
}