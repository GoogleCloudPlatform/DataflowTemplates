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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ProtoDecodeTransformer}, focusing on {@code transformBounded}. */
@RunWith(JUnit4.class)
public class ProtoDecodeTransformerTest {

  @Test
  public void transformBoundedSuccessOnSmallMessage() throws Exception {
    Descriptor descriptor = buildSimpleMessageDescriptor();
    ProtoDecodeTransformer transformer = new ProtoDecodeTransformer(descriptor, false);

    byte[] protoBytes =
        DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName("user_name"), "alice")
            .setField(descriptor.findFieldByName("id"), 7)
            .build()
            .toByteArray();

    TransformResult result = transformer.transformBounded(protoBytes, 10_000);
    assertEquals(TransformResult.Status.SUCCESS, result.status());
    assertNotNull(result.value());
    assertTrue("expected JSON output, got: " + result.value(), result.value().contains("alice"));
    assertEquals(protoBytes.length, result.rawBytes());
    assertEquals(result.value().length(), result.decodedBytes());
  }

  @Test
  public void transformBoundedShortCircuitsOnObviouslyLargePayloads() throws Exception {
    Descriptor descriptor = buildSimpleMessageDescriptor();
    ProtoDecodeTransformer transformer = new ProtoDecodeTransformer(descriptor, false);

    // maxBytes = 10 -> any raw bytes whose 4/3 estimate exceeds 10 should short-circuit.
    // 10 * 3 / 4 = 7, so anything > 7 bytes triggers the short circuit without parsing proto.
    byte[] fakeBytes = new byte[100];
    TransformResult result = transformer.transformBounded(fakeBytes, 10);
    assertEquals(TransformResult.Status.OVERSIZED, result.status());
    assertEquals(100L, result.rawBytes());
    assertNull(result.value());
  }

  @Test
  public void transformBoundedShortCircuitOversizedViaCheapPreCheck() throws Exception {
    Descriptor descriptor = buildSimpleMessageDescriptor();
    ProtoDecodeTransformer transformer = new ProtoDecodeTransformer(descriptor, false);

    // Raw bytes large enough that raw.length * 4 / 3 clearly exceeds maxBytes, so the pre-check
    // path triggers without attempting a full parse.
    byte[] fakeBytes = new byte[9_000];
    long maxBytes = 1_000;
    TransformResult result = transformer.transformBounded(fakeBytes, maxBytes);
    assertEquals(TransformResult.Status.OVERSIZED, result.status());
    assertEquals(fakeBytes.length, result.rawBytes());
    assertTrue(
        "expected estimated decoded bytes to exceed maxBytes", result.decodedBytes() > maxBytes);
    assertNull(result.value());
  }

  @Test
  public void transformBoundedDetectsOversizedDuringSerialization() throws Exception {
    // Use a repeated int32 field so the proto wire form is very compact (packed varints) while
    // the JSON form is verbose ({"values":[0,1,2,...]}), forcing appendTo to overflow mid-decode
    // rather than the cheap pre-check catching it.
    Descriptor descriptor = buildRepeatedIntsDescriptor();
    ProtoDecodeTransformer transformer = new ProtoDecodeTransformer(descriptor, false);

    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
    for (int i = 0; i < 500; i++) {
      builder.addRepeatedField(descriptor.findFieldByName("values"), i);
    }
    byte[] protoBytes = builder.build().toByteArray();

    // Cap at raw * 4 / 3 + 10 so the cheap pre-check passes, then the mid-decode appendable
    // should overflow once enough ints have been serialized.
    long cheapEstimate = (long) protoBytes.length * 4L / 3L;
    long maxBytes = cheapEstimate + 10;

    TransformResult result = transformer.transformBounded(protoBytes, maxBytes);
    assertEquals(
        "pre-check should pass (cheapEstimate="
            + cheapEstimate
            + ", maxBytes="
            + maxBytes
            + "), appendTo should then overflow",
        TransformResult.Status.OVERSIZED,
        result.status());
    assertEquals(protoBytes.length, result.rawBytes());
    assertTrue(result.decodedBytes() > maxBytes);
  }

  @Test
  public void transformBoundedReturnsDecodeErrorOnMalformedBytes() throws Exception {
    Descriptor descriptor = buildSimpleMessageDescriptor();
    ProtoDecodeTransformer transformer = new ProtoDecodeTransformer(descriptor, false);

    // Garbage bytes that cannot parse as SimpleMessage.
    byte[] garbage = new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
    TransformResult result = transformer.transformBounded(garbage, 10_000);
    assertEquals(TransformResult.Status.DECODE_ERROR, result.status());
    assertEquals(garbage.length, result.rawBytes());
    assertNull(result.value());
  }

  @Test
  public void transformBoundedWithZeroMaxBytesIsUnbounded() throws Exception {
    Descriptor descriptor = buildSimpleMessageDescriptor();
    ProtoDecodeTransformer transformer = new ProtoDecodeTransformer(descriptor, false);

    byte[] protoBytes =
        DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName("user_name"), "x".repeat(1_000))
            .setField(descriptor.findFieldByName("id"), 1)
            .build()
            .toByteArray();

    TransformResult result = transformer.transformBounded(protoBytes, 0L);
    assertEquals(TransformResult.Status.SUCCESS, result.status());
    assertNotNull(result.value());
  }

  /** Builds a simple proto descriptor with two scalar fields: user_name (string) and id (int32). */
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

  /** Builds a descriptor with a single repeated int32 field. */
  private Descriptor buildRepeatedIntsDescriptor() throws DescriptorValidationException {
    FileDescriptorProto fileProto =
        FileDescriptorProto.newBuilder()
            .setName("test_repeated.proto")
            .setPackage("test")
            .addMessageType(
                DescriptorProto.newBuilder()
                    .setName("RepeatedIntsMessage")
                    .addField(
                        FieldDescriptorProto.newBuilder()
                            .setName("values")
                            .setNumber(1)
                            .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                            .setType(FieldDescriptorProto.Type.TYPE_INT32)
                            .build())
                    .build())
            .build();
    FileDescriptor fileDescriptor = FileDescriptor.buildFrom(fileProto, new FileDescriptor[] {});
    return fileDescriptor.findMessageTypeByName("RepeatedIntsMessage");
  }
}
