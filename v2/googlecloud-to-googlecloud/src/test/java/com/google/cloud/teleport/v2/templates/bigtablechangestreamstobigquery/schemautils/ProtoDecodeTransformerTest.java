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
  public void transformBoundedAllowsStringHeavyMessageThatFitsJsonBudget() throws Exception {
    Descriptor descriptor = buildSimpleMessageDescriptor();
    ProtoDecodeTransformer transformer = new ProtoDecodeTransformer(descriptor, false);

    byte[] protoBytes =
        DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName("user_name"), "x".repeat(10_000))
            .setField(descriptor.findFieldByName("id"), 7)
            .build()
            .toByteArray();

    String expectedJson = transformer.transform(protoBytes);
    long maxBytes = expectedJson.length();

    // This payload used to be rejected by the old raw*4/3 heuristic even though the rendered JSON
    // still fits within maxBytes.
    assertTrue(((long) protoBytes.length * 4L) / 3L > maxBytes);

    TransformResult result = transformer.transformBounded(protoBytes, maxBytes);
    assertEquals(TransformResult.Status.SUCCESS, result.status());
    assertEquals(expectedJson, result.value());
    assertEquals(protoBytes.length, result.rawBytes());
    assertEquals(maxBytes, result.decodedBytes());
  }

  @Test
  public void transformBoundedShortCircuitsWhenRawPayloadAlreadyExceedsBudget() throws Exception {
    Descriptor descriptor = buildSimpleMessageDescriptor();
    ProtoDecodeTransformer transformer = new ProtoDecodeTransformer(descriptor, false);

    byte[] fakeBytes = new byte[101];
    TransformResult result = transformer.transformBounded(fakeBytes, 100);
    assertEquals(TransformResult.Status.OVERSIZED, result.status());
    assertEquals(101L, result.rawBytes());
    assertEquals(101L, result.decodedBytes());
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

    // Keep the cap above the raw protobuf wire size so we do not fast-reject before serialization.
    long maxBytes = protoBytes.length + 10;

    TransformResult result = transformer.transformBounded(protoBytes, maxBytes);
    assertEquals(
        "raw size should pass the pre-check and appendTo should overflow during JSON rendering",
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
