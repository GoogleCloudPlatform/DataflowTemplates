/*
 * Copyright (C) 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;

/**
 * Deterministic binary coder for {@link MongoDbChangeEventContext}.
 *
 * <p>Encodes MongoDB change event context without Java reflection serialization overhead,
 * preserving current and original payloads (for UDF transformations and DLQ auditing), collection
 * names, DLQ reconsumption flags, and retry counters.
 */
public class MongoDbChangeEventContextCoder extends AtomicCoder<MongoDbChangeEventContext> {

  private static final MongoDbChangeEventContextCoder INSTANCE =
      new MongoDbChangeEventContextCoder();
  private static final NullableCoder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
  private static final BooleanCoder BOOLEAN_CODER = BooleanCoder.of();
  private static final VarIntCoder VARINT_CODER = VarIntCoder.of();

  private MongoDbChangeEventContextCoder() {}

  public static MongoDbChangeEventContextCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(MongoDbChangeEventContext value, OutputStream outStream) throws IOException {
    if (value == null) {
      BOOLEAN_CODER.encode(false, outStream);
      return;
    }
    BOOLEAN_CODER.encode(true, outStream);
    STRING_CODER.encode(value.getChangeEventJsonString(), outStream);
    STRING_CODER.encode(value.getOriginalChangeEventJsonString(), outStream);
    STRING_CODER.encode(value.getShadowCollectionPrefix(), outStream);
    BOOLEAN_CODER.encode(value.getIsDlqReconsumed(), outStream);
    VARINT_CODER.encode(value.getRetryCount(), outStream);
  }

  @Override
  public MongoDbChangeEventContext decode(InputStream inStream) throws IOException {
    boolean isPresent = BOOLEAN_CODER.decode(inStream);
    if (!isPresent) {
      return null;
    }
    String changeEventJson = STRING_CODER.decode(inStream);
    String originalChangeEventJson = STRING_CODER.decode(inStream);
    String shadowPrefix = STRING_CODER.decode(inStream);
    boolean isDlq = BOOLEAN_CODER.decode(inStream);
    int retryCount = VARINT_CODER.decode(inStream);

    return MongoDbChangeEventContext.reconstitute(
        changeEventJson, originalChangeEventJson, shadowPrefix, isDlq, retryCount);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    STRING_CODER.verifyDeterministic();
    BOOLEAN_CODER.verifyDeterministic();
    VARINT_CODER.verifyDeterministic();
  }
}
