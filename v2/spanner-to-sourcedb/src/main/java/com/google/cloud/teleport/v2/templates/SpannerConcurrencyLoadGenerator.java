package com.google.cloud.teleport.v2.templates;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.List;

public class SpannerConcurrencyLoadGenerator {

    public interface LoadGeneratorOptions extends PipelineOptions {
        @Description("Project ID for Spanner")
        @Required
        String getProjectId();

        void setProjectId(String value);

        @Description("Spanner Instance ID")
        @Required
        String getInstanceId();

        void setInstanceId(String value);

        @Description("Spanner Database ID")
        @Required
        String getDatabaseId();

        void setDatabaseId(String value);
    }

    public static void main(String[] args) {
        LoadGeneratorOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(LoadGeneratorOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        // We want exactly 10,000 updates per second total (100 updates per second on
        // 100 keys).
        // To prevent Beam's SpannerIO from deduplicating or merging batches, we
        // generate exactly 10,000 elements per second.
        // For each element, a custom DoFn will execute a single Spanner transaction
        // containing 1 update.
        pipeline.apply("Generate Sequence", GenerateSequence.from(0).withRate(10000, Duration.standardSeconds(1)))
                .apply("Reshuffle", Reshuffle.viaRandomKey())
                .apply("Write to Spanner", ParDo.of(new WriteMutationsFn(options.getProjectId(),
                        options.getInstanceId(), options.getDatabaseId())));

        pipeline.run();
    }

    static class WriteMutationsFn extends DoFn<Long, Void> {
        private final String projectId;
        private final String instanceId;
        private final String databaseId;
        private transient Spanner spanner;
        private transient DatabaseClient dbClient;

        public WriteMutationsFn(String projectId, String instanceId, String databaseId) {
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
        }

        @Setup
        public void setup() {
            SpannerOptions spannerOptions = SpannerOptions.newBuilder().setProjectId(projectId).build();
            spanner = spannerOptions.getService();
            DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);
            dbClient = spanner.getDatabaseClient(db);
        }

        @Teardown
        public void teardown() {
            if (spanner != null) {
                spanner.close();
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            Long element = c.element();
            long id = element % 100;

            Mutation mutation = Mutation.newInsertOrUpdateBuilder("Users")
                    .set("Id").to(id)
                    .set("FirstName").to("FirstName_" + element)
                    .set("LastName").to("LastName_" + element + "_" + id)
                    .set("Email").to("user_" + id + "@example.com")
                    .set("Status").to("UPDATED")
                    .set("LastUpdated").to(Value.COMMIT_TIMESTAMP)
                    .build();

            // Execute as a single transaction directly against Spanner, bypassing any Beam
            // SpannerIO batching logic
            dbClient.writeAtLeastOnce(List.of(mutation));
        }
    }
}
