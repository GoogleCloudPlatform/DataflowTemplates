package com.mw.pipeline.options;

import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;
import org.apache.beam.sdk.options.*;

public interface StreamOptions extends SnowflakePipelineOptions {
    @Description("The Cloud Pub/Sub subscription to consume from. "
            + "Format: projects/<project-id>/subscriptions/<subscription-name>.")
    @Validation.Required
    ValueProvider<String> getInputSubscription();

    void setInputSubscription(ValueProvider<String> inputSubscription);

    // KMS Encryption Key
    @Description(
            "KMS Encryption Key should be in the format projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}")
    ValueProvider<String> getKMSEncryptionKey();

    void setKMSEncryptionKey(ValueProvider<String> keyName);
}
