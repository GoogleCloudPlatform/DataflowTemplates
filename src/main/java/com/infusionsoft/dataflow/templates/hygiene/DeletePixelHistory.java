package com.infusionsoft.dataflow.templates.hygiene;

import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubReadOptions;
import com.google.datastore.v1.Entity;
import com.infusionsoft.dataflow.shared.EntityToKey;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

/**
 * A template that deletes pixel history by accountId.
 *
 * Used by tracking-pixel-api
 *
 * Deploy to sand:
 * mvn compile exec:java -Dexec.mainClass=com.infusionsoft.dataflow.templates.hygiene.DeletePixelHistory -Dexec.args="--project=is-tracking-pixel-api-sand --stagingLocation=gs://dataflow-is-tracking-pixel-api-sand/staging --templateLocation=gs://dataflow-is-tracking-pixel-api-sand/templates/delete_pixels --runner=DataflowRunner --serviceAccount=is-tracking-pixel-api-sand@appspot.gserviceaccount.com --datastoreProjectId=is-tracking-pixel-api-sand"
 *
 * Deploy to prod:
 * mvn compile exec:java -Dexec.mainClass=com.infusionsoft.dataflow.templates.hygiene.DeletePixelHistory -Dexec.args="--project=is-tracking-pixel-api-prod --stagingLocation=gs://dataflow-is-tracking-pixel-api-prod/staging --templateLocation=gs://dataflow-is-tracking-pixel-api-prod/templates/delete_pixels --runner=DataflowRunner --serviceAccount=is-tracking-pixel-api-prod@appspot.gserviceaccount.com --datastoreProjectId=is-tracking-pixel-api-prod"
 *
 */
public class DeletePixelHistory {

  public interface Options extends PipelineOptions, StreamingOptions, PubsubReadOptions {

    @Description("GCP Project Id of where the datastore entities live")
    ValueProvider<String> getDatastoreProjectId();
    void setDatastoreProjectId(ValueProvider<String> datastoreProjectId);

    @Description("The Account Id whose emails are being deleted")
    ValueProvider<String> getAccountId();
    void setAccountId(ValueProvider<String> accountId);

  }

  public static void main(String[] args) {
    final Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    final String projectId = options.getDatastoreProjectId().get();

    final Pipeline pipeline = Pipeline.create(options);

    final PCollection<Entity> pixels = pipeline.apply("Find Pixels", DatastoreIO.v1().read()
        .withProjectId(projectId)
        .withLiteralGqlQuery(NestedValueProvider.of(options.getAccountId(),
            (SerializableFunction<String, String>) accountId -> String.format("SELECT __key__ FROM Pixel WHERE accountUid = '%s'", accountId))));

    final PCollection<Entity> renders = pipeline.apply("Find Renders", DatastoreIO.v1().read()
        .withProjectId(projectId)
        .withLiteralGqlQuery(NestedValueProvider.of(options.getAccountId(),
            (SerializableFunction<String, String>) accountId -> String.format("SELECT __key__ FROM Render WHERE accountUid = '%s'", accountId))));

    final PCollection<Entity> uniqueRenders = pipeline.apply("Find UniqueRenders", DatastoreIO.v1().read()
        .withProjectId(projectId)
        .withLiteralGqlQuery(NestedValueProvider.of(options.getAccountId(),
            (SerializableFunction<String, String>) accountId -> String.format("SELECT __key__ FROM UniqueRender WHERE accountUid = '%s'", accountId))));

    final PCollection<Entity> contacts = pipeline.apply("Find Contacts", DatastoreIO.v1().read()
        .withProjectId(projectId)
        .withLiteralGqlQuery(NestedValueProvider.of(options.getAccountId(),
            (SerializableFunction<String, String>) accountId -> String.format("SELECT __key__ FROM Contact WHERE accountUid = '%s'", accountId))));

    final PCollection<Entity> uniqueContacts = pipeline.apply("Find UniqueContacts", DatastoreIO.v1().read()
        .withProjectId(projectId)
        .withLiteralGqlQuery(NestedValueProvider.of(options.getAccountId(),
            (SerializableFunction<String, String>) accountId -> String.format("SELECT __key__ FROM UniqueContact WHERE accountUid = '%s'", accountId))));

    final PCollection<Entity> composite = PCollectionList.of(pixels)
        .and(renders).and(uniqueRenders)
        .and(contacts).and(uniqueContacts)
        .apply(Flatten.pCollections());

    composite
        .apply("Shard", Reshuffle.viaRandomKey()) // this ensures that the subsequent steps occur in parallel
        .apply("Entity To Key", ParDo.of(new EntityToKey()))
        .apply("Delete By Key", DatastoreIO.v1().deleteKey()
            .withProjectId(projectId));

    pipeline.run();
  }
}