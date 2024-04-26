package com.google.cloud.teleport.v2.coders;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/**
 * An {@link AtomicCoder} for {@link GenericRecord}.
 *
 * <p>This coder is used when the schema of the incoming {@link GenericRecord} can only be known at
 * runtime.
 */
public class GenericRecordCoder extends AtomicCoder<GenericRecord> {
    private static final Integer MAX_CACHE_SIZE = 1000;

    /** Constructs a new {@link GenericRecordCoder} */
    public static GenericRecordCoder of() {
        return new GenericRecordCoder();
    }

    // Keep a cache for {@link AvroCoder} to avoid reconstructing repeatedly.
    // In our use case, the schema should be the same for each record, so the benefit of caching is
    // significant.
    private static final Cache<String, AvroCoder<GenericRecord>> avroCoderCache =
            CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE).build();

    @Override
    public void encode(GenericRecord value, OutputStream outStream) throws IOException {
        String schemaString = value.getSchema().toString();
        StringUtf8Coder.of().encode(schemaString, outStream);
        AvroCoder<GenericRecord> coder = getAvroCoder(value.getSchema().toString());
        coder.encode(value, outStream);
    }

    @Override
    public GenericRecord decode(InputStream inStream) throws IOException {
        String schemaString = StringUtf8Coder.of().decode(inStream);
        AvroCoder<GenericRecord> coder = getAvroCoder(schemaString);
        return coder.decode(inStream);
    }

    private AvroCoder<GenericRecord> getAvroCoder(String schemaString) {
        try {
            return avroCoderCache.get(
                    schemaString, () -> AvroCoder.of(new Schema.Parser().parse(schemaString)));
        } catch (ExecutionException e) {
            throw new AssertionError("impossible; loader can't throw.");
        }
    }
}
