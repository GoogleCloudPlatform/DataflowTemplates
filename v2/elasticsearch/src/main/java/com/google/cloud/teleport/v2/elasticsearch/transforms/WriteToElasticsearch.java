package com.google.cloud.teleport.v2.elasticsearch.transforms;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.elasticsearch.options.ElasticsearchOptions;
import com.google.cloud.teleport.v2.transforms.ValueExtractorTransform;
import java.util.Optional;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;




/**
 * The {@link WriteToElasticsearch} class writes a {@link PCollection} of strings to Elasticsearch
 * using the following options.
 *
 * <ul>
 *   <li>{@link ElasticsearchOptions#getNodeAddresses()} - comma separated list of nodes.
 *   <li>{@link ElasticsearchOptions#getIndex()} - index to output documents to.
 *   <li>{@link ElasticsearchOptions#getDocumentType()} - document type to write to.
 *   <li>{@link ElasticsearchOptions#getBatchSize()} - batch size in number of documents
 *       (Default:1000).
 *   <li>{@link ElasticsearchOptions#getBatchSizeBytes()} - batch size in number of bytes
 *       (Default:5242880).
 *   <li>{@link ElasticsearchOptions#getMaxRetryAttempts()} - optional: maximum retry
 *       attempts for {@link ElasticsearchIO.RetryConfiguration}.
 *   <li>{@link ElasticsearchOptions#getMaxRetryDuration()} - optional: maximum retry
 *       duration for {@link ElasticsearchIO.RetryConfiguration}.
 *   <li>{@link ElasticsearchOptions#getUsePartialUpdate()} - use partial updates instead
 *       of insertions (Default: false).
 * </ul>
 * <p>
 * For {@link ElasticsearchIO#write()} with {@link ValueExtractorTransform.ValueExtractorFn} if the function returns null
 * then the index or type provided as {@link ElasticsearchOptions#getIndex()} or {@link
 * ElasticsearchOptions#getDocumentType()} will be used. For IdFn if function returns null
 * then the id for the document will be assigned by {@link ElasticsearchIO}.
 */
@AutoValue
public abstract class WriteToElasticsearch extends PTransform<PCollection<String>, PDone> {

    /**
     * Convert provided long to {@link Duration}.
     */
    private static Duration getDuration(Long milliseconds) {
        return new Duration(milliseconds);
    }

    public static Builder newBuilder() {
        return new AutoValue_WriteToElasticsearch.Builder();
    }

    public abstract ElasticsearchOptions options();

    @Override
    public PDone expand(PCollection<String> jsonStrings) {

        ElasticsearchIO.ConnectionConfiguration config =
                ElasticsearchIO.ConnectionConfiguration.create(
                        options().getNodeAddresses().split(","),
                        options().getIndex(),
                        options().getDocumentType())
                        .withUsername(options().getElasticsearchUsername())
                        .withPassword(options().getElasticsearchPassword());

        ElasticsearchIO.Write write =
                ElasticsearchIO.write()
                        .withConnectionConfiguration(config)
                        .withMaxBatchSize(options().getBatchSize())
                        .withMaxBatchSizeBytes(options().getBatchSizeBytes())
                        .withUsePartialUpdate(options().getUsePartialUpdate());

        if (Optional.ofNullable(options().getMaxRetryAttempts()).isPresent()) {
            write.withRetryConfiguration(
                    ElasticsearchIO.RetryConfiguration.create(
                            options().getMaxRetryAttempts(), getDuration(options().getMaxRetryDuration())));
        }

        return jsonStrings.apply(
                "WriteDocuments",
                write
                        .withIdFn(
                                ValueExtractorTransform.ValueExtractorFn.newBuilder()
                                        .setFileSystemPath(options().getIdFnPath())
                                        .setFunctionName(options().getIdFnName())
                                        .build())
                        .withIndexFn(
                                ValueExtractorTransform.ValueExtractorFn.newBuilder()
                                        .setFileSystemPath(options().getIndexFnPath())
                                        .setFunctionName(options().getIndexFnName())
                                        .build())
                        .withTypeFn(
                                ValueExtractorTransform.ValueExtractorFn.newBuilder()
                                        .setFileSystemPath(options().getTypeFnPath())
                                        .setFunctionName(options().getTypeFnName())
                                        .build()));
    }

    /**
     * Builder for {@link WriteToElasticsearch}.
     */
    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setOptions(ElasticsearchOptions options);

        abstract ElasticsearchOptions options();

        abstract WriteToElasticsearch autoBuild();

        public WriteToElasticsearch build() {

            checkArgument(
                    options().getNodeAddresses() != null,
                    "Node address(es) must not be null.");

            checkArgument(options().getDocumentType() != null,
                    "Document type must not be null.");

            checkArgument(options().getIndex() != null,
                    "Index must not be null.");

            checkArgument(
                    options().getBatchSize() > 0,
                    "Batch size must be > 0. Got: " + options().getBatchSize());

            checkArgument(
                    options().getBatchSizeBytes() > 0,
                    "Batch size bytes must be > 0. Got: " + options().getBatchSizeBytes());

            /* Check that both {@link RetryConfiguration} parameters are supplied. */
            if (options().getMaxRetryAttempts() != null
                    || options().getMaxRetryDuration() != null) {
                checkArgument(
                        options().getMaxRetryDuration() != null
                                && options().getMaxRetryAttempts() != null,
                        "Both max retry duration and max attempts must be supplied.");
            }

            if (options().getIdFnName() != null || options().getIdFnPath() != null) {
                checkArgument(
                        options().getIdFnName() != null && options().getIdFnPath() != null,
                        "Both IdFn name and path must be supplied.");
            }

            if (options().getIndexFnName() != null || options().getIndexFnPath() != null) {
                checkArgument(
                        options().getIndexFnName() != null && options().getIndexFnPath() != null,
                        "Both IndexFn name and path must be supplied.");
            }

            if (options().getTypeFnName() != null || options().getTypeFnPath() != null) {
                checkArgument(
                        options().getTypeFnName() != null && options().getTypeFnPath() != null,
                        "Both TypeFn name and path must be supplied.");
            }

            return autoBuild();
        }
    }
}
