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
package com.google.cloud.teleport.v2.neo4j.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serial;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.values.Row;
import org.jspecify.annotations.NonNull;

/**
 * Coder for {@link Row rows} whose schema is only known at pipeline execution time.
 *
 * <p>Beam's regular {@link RowCoder} needs a schema when the pipeline graph is built. Source reads
 * whose schema can only be resolved by a worker (for instance BigQuery queries) cannot provide one
 * at that point. This coder therefore writes the schema along with each element, so that decoding
 * does not depend on any build-time information.
 *
 * <p>The schema is encoded with Beam's portable schema protobuf rather than Java serialization: it
 * is substantially more compact and does not embed the random {@link Schema#getUUID() UUID} that
 * every {@code Schema} instance carries.
 */
public class RuntimeSchemaRowCoder extends Coder<Row> {

  public static final RuntimeSchemaRowCoder INSTANCE = new RuntimeSchemaRowCoder();

  @Serial private static final long serialVersionUID = 1L;

  private static final VarIntCoder LENGTH_CODER = VarIntCoder.of();

  // A pipeline only ever sees a handful of distinct schemas; caching avoids re-serializing the
  // same schema for every encoded element and reparsing it for every decoded element. The caches
  // are bounded with an approximate-LRU eviction policy to guard against pathological schema
  // churn: evicting an entry costs recomputation, never correctness, because the schema travels
  // with each element. Cache hits must not serialize the hot path, since every harness thread
  // encodes and decodes elements.
  @VisibleForTesting static final int MAX_CACHED_SCHEMAS = 64;

  private static final Cache<Schema, EncodedSchema> ENCODERS =
      CacheBuilder.newBuilder().maximumSize(MAX_CACHED_SCHEMAS).build();

  private static final Cache<SchemaKey, EncodedSchema> DECODERS =
      CacheBuilder.newBuilder().maximumSize(MAX_CACHED_SCHEMAS).build();

  @Override
  public void encode(Row value, @NonNull OutputStream out) throws IOException {
    var encodedSchema = encodedFor(value.getSchema());
    LENGTH_CODER.encode(encodedSchema.bytes.length, out);
    out.write(encodedSchema.bytes);
    encodedSchema.coder.encode(value, out);
  }

  @Override
  public Row decode(@NonNull InputStream in) throws IOException {
    int schemaSize = LENGTH_CODER.decode(in);
    var serializedSchema = new byte[schemaSize];
    ByteStreams.readFully(in, serializedSchema);
    var key = new SchemaKey(serializedSchema);
    var encodedSchema = DECODERS.getIfPresent(key);
    if (encodedSchema == null) {
      try {
        encodedSchema =
            DECODERS.get(
                key,
                () -> {
                  var loaded = EncodedSchema.fromProtoBytes(serializedSchema);
                  // Decoded rows are re-encoded downstream: warm the encoder cache too. The loader
                  // runs once per distinct schema, even when threads race on the same miss.
                  ENCODERS.put(loaded.schema, loaded);
                  return loaded;
                });
      } catch (ExecutionException e) {
        if (e.getCause() instanceof IOException cause) {
          throw cause;
        }
        throw new IOException("Unable to decode the row schema", e.getCause());
      }
    }
    return encodedSchema.coder.decode(in);
  }

  @Override
  public Object structuralValue(Row value) {
    // Delegating to the schema's RowCoder avoids re-encoding the schema bytes on every structural
    // comparison. Equal rows under equal schemas encode identically, so this remains consistent
    // with encoded equality.
    return encodedFor(value.getSchema()).coder.structuralValue(value);
  }

  @Override
  public void verifyDeterministic() {
    // For a given schema, encoding is deterministic: the schema protobuf deliberately omits the
    // random Schema UUID, and Beam-provided logical types serialize deterministically.
  }

  @Override
  public @NonNull List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof RuntimeSchemaRowCoder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(RuntimeSchemaRowCoder.class);
  }

  @VisibleForTesting
  static int cachedEncoderCount() {
    return (int) ENCODERS.size();
  }

  @VisibleForTesting
  static int cachedDecoderCount() {
    return (int) DECODERS.size();
  }

  @VisibleForTesting
  static void resetCaches() {
    ENCODERS.invalidateAll();
    DECODERS.invalidateAll();
  }

  private static EncodedSchema encodedFor(Schema schema) {
    var encodedSchema = ENCODERS.getIfPresent(schema);
    if (encodedSchema == null) {
      // Put through the map view so that size-based eviction still applies.
      encodedSchema = ENCODERS.asMap().computeIfAbsent(schema, EncodedSchema::new);
    }
    return encodedSchema;
  }

  /** Content-based key for a serialized schema, so decoding can hit the cache before parsing. */
  private record SchemaKey(byte[] bytes) {

    @Override
    public boolean equals(Object o) {
      return o instanceof SchemaKey && Arrays.equals(bytes, ((SchemaKey) o).bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }
  }

  private static final class EncodedSchema {
    private final Schema schema;
    private final byte[] bytes;
    private final RowCoder coder;

    private EncodedSchema(Schema schema) {
      this(schema, SchemaTranslation.schemaToProto(schema, true, false).toByteArray());
    }

    private EncodedSchema(Schema schema, byte[] bytes) {
      this.schema = schema;
      this.bytes = bytes;
      this.coder = RowCoder.of(schema);
    }

    private static EncodedSchema fromProtoBytes(byte[] bytes) throws IOException {
      try {
        Schema schema = SchemaTranslation.schemaFromProto(SchemaApi.Schema.parseFrom(bytes));
        return new EncodedSchema(schema, bytes);
      } catch (IOException e) {
        throw new IOException("Unable to decode the row schema", e);
      }
    }
  }
}
