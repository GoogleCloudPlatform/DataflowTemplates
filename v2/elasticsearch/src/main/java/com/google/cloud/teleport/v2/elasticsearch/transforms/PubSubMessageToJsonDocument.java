package com.google.cloud.teleport.v2.elasticsearch.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.elasticsearch.templates.PubSubToElasticsearch;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;


/**
 * The {@link PubSubMessageToJsonDocument} class is a {@link PTransform} which transforms incoming
 * {@link PubsubMessage} objects into JSON objects for insertion into Elasticsearch while applying
 * an optional UDF to the input. The executions of the UDF and transformation to Json objects is
 * done in a fail-safe way by wrapping the element with it's original payload inside the {@link
 * FailsafeElement} class. The {@link PubSubMessageToJsonDocument} transform will output a {@link
 * PCollectionTuple} which contains all output and dead-letter {@link PCollection}.
 *
 * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
 *
 * <ul>
 *   <li>{@link PubSubToElasticsearch#TRANSFORM_OUT} - Contains all records successfully converted
 *       to JSON objects.
 *   <li>{@link PubSubToElasticsearch#TRANSFORM_DEADLETTER_OUT} - Contains all {@link
 *       FailsafeElement} records which couldn't be converted to table rows.
 * </ul>
 */
@AutoValue
public abstract class PubSubMessageToJsonDocument
        extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

    public static Builder newBuilder() {
        return new AutoValue_PubSubMessageToJsonDocument.Builder();
    }

    @Nullable
    public abstract String javascriptTextTransformGcsPath();

    @Nullable
    public abstract String javascriptTextTransformFunctionName();

    @Override
    public PCollectionTuple expand(PCollection<PubsubMessage> input) {

        // Map the incoming messages into FailsafeElements so we can recover from failures
        // across multiple transforms.
        PCollection<FailsafeElement<PubsubMessage, String>> failsafeElements =
                input.apply("MapToRecord", ParDo.of(new PubsubMessageToFailsafeElementFn()));

        // If a Udf is supplied then use it to parse the PubSubMessages.
        if (javascriptTextTransformGcsPath() != null) {
            return failsafeElements.apply(
                    "InvokeUDF",
                    JavascriptTextTransformer.FailsafeJavascriptUdf.<PubsubMessage>newBuilder()
                            .setFileSystemPath(javascriptTextTransformGcsPath())
                            .setFunctionName(javascriptTextTransformFunctionName())
                            .setSuccessTag(PubSubToElasticsearch.TRANSFORM_OUT)
                            .setFailureTag(PubSubToElasticsearch.TRANSFORM_DEADLETTER_OUT)
                            .build());
        } else {
            return failsafeElements.apply(
                    "ProcessPubSubMessages",
                    ParDo.of(new ProcessFailsafePubSubFn())
                            .withOutputTags(PubSubToElasticsearch.TRANSFORM_OUT, TupleTagList.of(PubSubToElasticsearch.TRANSFORM_DEADLETTER_OUT)));
        }
    }

    /**
     * Builder for {@link PubSubMessageToJsonDocument}.
     */
    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setJavascriptTextTransformGcsPath(
                String javascriptTextTransformGcsPath);

        public abstract Builder setJavascriptTextTransformFunctionName(
                String javascriptTextTransformFunctionName);

        public abstract PubSubMessageToJsonDocument build();
    }
}
