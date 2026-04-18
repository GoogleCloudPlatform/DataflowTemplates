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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ValueTransformerRegistry#transformBounded}. */
@RunWith(JUnit4.class)
public class ValueTransformerRegistryTest {

  @Test
  public void protoDecodeDispatchesToBoundedImpl() throws Exception {
    Descriptor descriptor = buildSimpleMessageDescriptor();
    ValueTransformerRegistry registry =
        ValueTransformerRegistry.of(
            Map.of("cf:proto_col", new ProtoDecodeTransformer(descriptor, false)));

    byte[] protoBytes =
        DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName("user_name"), "alice")
            .setField(descriptor.findFieldByName("id"), 42)
            .build()
            .toByteArray();

    TransformResult ok = registry.transformBounded("cf", "proto_col", protoBytes, 10_000);
    assertEquals(TransformResult.Status.SUCCESS, ok.status());
    assertNotNull(ok.value());
    assertTrue(ok.value().contains("alice"));

    TransformResult oversized = registry.transformBounded("cf", "proto_col", new byte[9_000], 100);
    assertEquals(TransformResult.Status.OVERSIZED, oversized.status());
  }

  @Test
  public void bigEndianTimestampDispatchedViaFallback() {
    ValueTransformerRegistry registry =
        ValueTransformerRegistry.of(Map.of("cf:ts", new BigEndianTimestampTransformer()));

    byte[] eightBytes =
        ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(1_700_000_000_000L).array();
    TransformResult ok = registry.transformBounded("cf", "ts", eightBytes, 1_000);
    assertEquals(TransformResult.Status.SUCCESS, ok.status());
    assertNotNull(ok.value());
    assertEquals(eightBytes.length, ok.rawBytes());
    assertTrue(ok.decodedBytes() > 0);

    // Non-8-byte input -> transformer returns null -> DECODE_ERROR.
    TransformResult bad = registry.transformBounded("cf", "ts", new byte[] {1, 2, 3}, 1_000);
    assertEquals(TransformResult.Status.DECODE_ERROR, bad.status());
  }

  @Test
  public void missingTransformerReturnsNoTransformer() {
    ValueTransformerRegistry registry = ValueTransformerRegistry.of(Map.of());
    TransformResult result = registry.transformBounded("cf", "missing", new byte[] {1}, 1_000);
    assertEquals(TransformResult.Status.NO_TRANSFORMER, result.status());
    assertNull(result.value());
  }

  @Test
  public void nonProtoTransformerOutputRespectsMaxBytes() {
    // BigEndianTimestamp produces a ~26-char timestamp string; with maxBytes=5 it should be
    // reported as OVERSIZED even though its raw input is small.
    ValueTransformerRegistry registry =
        ValueTransformerRegistry.of(Map.of("cf:ts", new BigEndianTimestampTransformer()));
    byte[] eightBytes =
        ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(1_700_000_000_000L).array();
    TransformResult result = registry.transformBounded("cf", "ts", eightBytes, 5);
    assertEquals(TransformResult.Status.OVERSIZED, result.status());
  }

  @Test
  public void defaultTransformBoundedRunsForCustomTransformer() {
    // A custom (non-proto) transformer exercising the default ValueTransformer#transformBounded
    // method: SUCCESS path with exact decoded-byte measurement.
    ValueTransformer echo = bytes -> "decoded:" + bytes.length;
    ValueTransformerRegistry registry = ValueTransformerRegistry.of(Map.of("cf:c", echo));
    byte[] raw = new byte[] {1, 2, 3};

    TransformResult ok = registry.transformBounded("cf", "c", raw, 1_000);
    assertEquals(TransformResult.Status.SUCCESS, ok.status());
    assertEquals("decoded:3", ok.value());
    assertEquals(raw.length, ok.rawBytes());
    assertEquals("decoded:3".length(), ok.decodedBytes());

    // DECODE_ERROR: transformer returns null.
    ValueTransformer nullReturner = bytes -> null;
    ValueTransformerRegistry nullRegistry =
        ValueTransformerRegistry.of(Map.of("cf:c", nullReturner));
    TransformResult err = nullRegistry.transformBounded("cf", "c", raw, 1_000);
    assertEquals(TransformResult.Status.DECODE_ERROR, err.status());
    assertEquals(raw.length, err.rawBytes());

    // OVERSIZED: decoded string overruns the byte budget.
    TransformResult over = registry.transformBounded("cf", "c", raw, 3);
    assertEquals(TransformResult.Status.OVERSIZED, over.status());
    assertTrue(over.decodedBytes() > 3);
  }

  private Descriptor buildSimpleMessageDescriptor() throws DescriptorValidationException {
    FileDescriptorProto fileProto =
        FileDescriptorProto.newBuilder()
            .setName("test.proto")
            .setPackage("test")
            .addMessageType(
                DescriptorProto.newBuilder()
                    .setName("SimpleMessage")
                    .addField(
                        FieldDescriptorProto.newBuilder()
                            .setName("user_name")
                            .setNumber(1)
                            .setType(FieldDescriptorProto.Type.TYPE_STRING)
                            .build())
                    .addField(
                        FieldDescriptorProto.newBuilder()
                            .setName("id")
                            .setNumber(2)
                            .setType(FieldDescriptorProto.Type.TYPE_INT32)
                            .build())
                    .build())
            .build();
    FileDescriptor fileDescriptor = FileDescriptor.buildFrom(fileProto, new FileDescriptor[] {});
    return fileDescriptor.findMessageTypeByName("SimpleMessage");
  }
}
