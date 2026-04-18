/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.schemautils;

import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ValueTransformer} that decodes protobuf-encoded byte values into JSON strings.
 *
 * <p>The transformer lazily initializes the proto {@link Descriptor} and {@link JsonFormat.Printer}
 * on first use, since these objects are not serializable and must be created on the worker after
 * deserialization.
 */
public class ProtoDecodeTransformer implements ValueTransformer {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ProtoDecodeTransformer.class);

  private final String protoSchemaPath;
  private final String fullMessageName;
  private final boolean preserveFieldNames;

  private volatile boolean initialized = false;
  private transient Descriptor descriptor;
  private transient JsonFormat.Printer printer;

  public ProtoDecodeTransformer(
      String protoSchemaPath, String fullMessageName, boolean preserveFieldNames) {
    this.protoSchemaPath = protoSchemaPath;
    this.fullMessageName = fullMessageName;
    this.preserveFieldNames = preserveFieldNames;
  }

  private void ensureInitialized() {
    if (initialized) {
      return;
    }
    synchronized (this) {
      if (initialized) {
        return;
      }
      descriptor = SchemaUtils.getProtoDomain(protoSchemaPath).getDescriptor(fullMessageName);
      if (descriptor == null) {
        throw new IllegalArgumentException(
            fullMessageName + " is not a recognized message in " + protoSchemaPath);
      }
      JsonFormat.Printer basePrinter = JsonFormat.printer();
      printer = preserveFieldNames ? basePrinter.preservingProtoFieldNames() : basePrinter;
      initialized = true;
    }
  }

  /** Package-private constructor for testing with a pre-built descriptor. */
  ProtoDecodeTransformer(Descriptor descriptor, boolean preserveFieldNames) {
    this.protoSchemaPath = null;
    this.fullMessageName = null;
    this.preserveFieldNames = preserveFieldNames;
    this.descriptor = descriptor;
    JsonFormat.Printer basePrinter = JsonFormat.printer();
    this.printer = preserveFieldNames ? basePrinter.preservingProtoFieldNames() : basePrinter;
    this.initialized = true;
  }

  /**
   * Decodes protobuf bytes into a JSON string.
   *
   * @param bytes the serialized protobuf message bytes
   * @return the JSON representation, or null if decoding fails
   */
  @Override
  public String transform(byte[] bytes) {
    ensureInitialized();
    try {
      DynamicMessage message = DynamicMessage.parseFrom(descriptor, bytes);
      return printer.print(message);
    } catch (InvalidProtocolBufferException e) {
      LOG.warn("Failed to decode protobuf message for {}: {}", fullMessageName, e.getMessage());
      return null;
    }
  }

  /**
   * Decodes protobuf bytes into a JSON string while enforcing a byte-size bound on the JSON output.
   *
   * <p>Behaviour:
   *
   * <ul>
   *   <li>{@code maxBytes <= 0} disables the bound and behaves like {@link #transform(byte[])}.
   *   <li>A cheap pre-check rejects payloads that cannot possibly fit (raw size * 4/3 &gt;
   *       maxBytes); those short-circuit to {@link TransformResult.Status#OVERSIZED} without ever
   *       parsing the proto.
   *   <li>Otherwise the printer writes into a {@link Utf8BoundedAppendable} that aborts
   *       mid-serialization once the UTF-8 byte budget is exceeded.
   *   <li>Malformed proto input yields {@link TransformResult.Status#DECODE_ERROR}.
   * </ul>
   *
   * @param bytes the serialized protobuf message bytes
   * @param maxBytes the maximum allowed UTF-8 byte size of the JSON output; {@code <= 0} disables
   *     the bound
   * @return a {@link TransformResult} describing the outcome
   */
  TransformResult transformBounded(byte[] bytes, long maxBytes) {
    ensureInitialized();
    long rawBytes = bytes.length;

    if (maxBytes > 0) {
      // Cheap pre-check: base64-ish pessimistic estimate.
      long estimated = rawBytes * 4L / 3L;
      if (estimated > maxBytes) {
        return TransformResult.oversized(rawBytes, estimated);
      }
    }

    try {
      DynamicMessage message = DynamicMessage.parseFrom(descriptor, bytes);
      Utf8BoundedAppendable appendable = new Utf8BoundedAppendable(maxBytes);
      try {
        printer.appendTo(message, appendable);
      } catch (OversizedJsonException e) {
        return TransformResult.oversized(rawBytes, e.bytesSoFar());
      } catch (IOException e) {
        // StringBuilder-backed Appendable should not throw IOException.
        LOG.warn("Unexpected IO error while serializing proto to JSON: {}", e.getMessage());
        return TransformResult.decodeError(rawBytes);
      }
      return TransformResult.success(appendable.toJson(), rawBytes, appendable.byteCount());
    } catch (InvalidProtocolBufferException e) {
      LOG.warn("Failed to decode protobuf message for {}: {}", fullMessageName, e.getMessage());
      return TransformResult.decodeError(rawBytes);
    }
  }
}
