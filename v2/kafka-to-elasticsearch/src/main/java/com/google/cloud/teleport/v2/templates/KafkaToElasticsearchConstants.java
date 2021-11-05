package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

/** Constant variables that are used across the template's parts. */
public class KafkaToElasticsearchConstants {

    /** The tag for the main output for the UDF. */
    public static final TupleTag<FailsafeElement<KV<String, String>, String>> UDF_OUT =
            new TupleTag<FailsafeElement<KV<String, String>, String>>() {};

    /** The tag for the dead-letter output of the udf. */
    public static final TupleTag<FailsafeElement<KV<String, String>, String>> UDF_DEADLETTER_OUT =
            new TupleTag<FailsafeElement<KV<String, String>, String>>() {};

    /* Config keywords */
    public static final String KAFKA_CREDENTIALS = "kafka";
    public static final String SSL_CREDENTIALS = "ssl";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String BUCKET = "bucket";

}
