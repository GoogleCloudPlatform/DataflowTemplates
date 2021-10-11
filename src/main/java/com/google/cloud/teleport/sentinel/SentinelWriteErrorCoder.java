package com.google.cloud.teleport.sentinel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A {@link org.apache.beam.sdk.coders.Coder} for {@link SentinelWriteError} objects. */
public class SentinelWriteErrorCoder extends AtomicCoder<SentinelWriteError> {
    
    private static final SentinelWriteErrorCoder Sentinel_WRITE_ERROR_CODER = new SentinelWriteErrorCoder();

    private static final TypeDescriptor<SentinelWriteError> TYPE_DESCRIPTOR =
        new TypeDescriptor<SentinelWriteError>() {};
    private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();
    private static final NullableCoder<String> STRING_NULLABLE_CODER =
        NullableCoder.of(STRING_UTF_8_CODER);
    private static final NullableCoder<Integer> INTEGER_NULLABLE_CODER =
        NullableCoder.of(BigEndianIntegerCoder.of());
  
    public static SentinelWriteErrorCoder of() {
      return Sentinel_WRITE_ERROR_CODER;
    }
  
    @Override
    public void encode(SentinelWriteError value, OutputStream out) throws CoderException, IOException {
      INTEGER_NULLABLE_CODER.encode(value.statusCode(), out);
      STRING_NULLABLE_CODER.encode(value.statusMessage(), out);
      STRING_NULLABLE_CODER.encode(value.payload(), out);
    }
  
    @Override
    public SentinelWriteError decode(InputStream in) throws CoderException, IOException {
  
      SentinelWriteError.Builder builder = SentinelWriteError.newBuilder();
  
      Integer statusCode = INTEGER_NULLABLE_CODER.decode(in);
      if (statusCode != null) {
        builder.withStatusCode(statusCode);
      }
  
      String statusMessage = STRING_NULLABLE_CODER.decode(in);
      if (statusMessage != null) {
        builder.withStatusMessage(statusMessage);
      }
  
      String payload = STRING_NULLABLE_CODER.decode(in);
      if (payload != null) {
        builder.withPayload(payload);
      }
  
      return builder.build();
    }
  
    @Override
    public TypeDescriptor<SentinelWriteError> getEncodedTypeDescriptor() {
      return TYPE_DESCRIPTOR;
    }
  
    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      throw new NonDeterministicException(
          this, "SentinelWriteError can hold arbitrary instances, which may be non-deterministic.");
    }
  }
  
