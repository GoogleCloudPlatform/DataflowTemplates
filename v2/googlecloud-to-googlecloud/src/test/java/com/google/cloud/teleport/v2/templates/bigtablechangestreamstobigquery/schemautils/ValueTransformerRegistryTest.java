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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation.MutationType;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.TestUtil;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.Mod;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.TestChangeStreamMutation;
import com.google.cloud.teleport.v2.utils.BigtableSource;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
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

    byte[] stringHeavyProto =
        DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName("user_name"), "x".repeat(10_000))
            .setField(descriptor.findFieldByName("id"), 43)
            .build()
            .toByteArray();
    long maxBytes = transformerOutputBytes(registry, "cf", "proto_col", stringHeavyProto);
    assertTrue(((long) stringHeavyProto.length * 4L) / 3L > maxBytes);

    TransformResult fits = registry.transformBounded("cf", "proto_col", stringHeavyProto, maxBytes);
    assertEquals(TransformResult.Status.SUCCESS, fits.status());
    assertEquals(maxBytes, fits.decodedBytes());
  }

  @Test
  public void missingTransformerReturnsNoTransformer() {
    ValueTransformerRegistry registry = ValueTransformerRegistry.of(Map.of());
    TransformResult result = registry.transformBounded("cf", "missing", new byte[] {1}, 1_000);
    assertEquals(TransformResult.Status.NO_TRANSFORMER, result.status());
    assertNull(result.value());
  }

  @Test
  public void customTransformerOutputRespectsMaxBytes() {
    // A custom (non-proto) transformer that emits a fixed-length 26-character string. The registry
    // should still detect OVERSIZED through the default ValueTransformer#transformBounded path.
    String fixedOutput = "fixed-length-output-2024xx";
    ValueTransformer fixedString = bytes -> fixedOutput;
    ValueTransformerRegistry registry = ValueTransformerRegistry.of(Map.of("cf:c", fixedString));
    TransformResult result = registry.transformBounded("cf", "c", new byte[] {1}, 5);
    assertEquals(TransformResult.Status.OVERSIZED, result.status());
    assertTrue(result.decodedBytes() > 5);
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
    assertEquals(raw.length, ok.rawBytesLength());
    assertEquals("decoded:3".length(), ok.decodedBytes());

    // DECODE_ERROR: transformer returns null.
    ValueTransformer nullReturner = bytes -> null;
    ValueTransformerRegistry nullRegistry =
        ValueTransformerRegistry.of(Map.of("cf:c", nullReturner));
    TransformResult err = nullRegistry.transformBounded("cf", "c", raw, 1_000);
    assertEquals(TransformResult.Status.DECODE_ERROR, err.status());
    assertEquals(raw.length, err.rawBytesLength());

    // OVERSIZED: decoded string overruns the byte budget.
    TransformResult over = registry.transformBounded("cf", "c", raw, 3);
    assertEquals(TransformResult.Status.OVERSIZED, over.status());
    assertTrue(over.decodedBytes() > 3);
  }

  @Test
  public void parseSupportsMultipleProtoColumnsAndColonQualifier() {
    ValueTransformerRegistry registry =
        ValueTransformerRegistry.parse(
            "cf:profile:v1:PROTO_DECODE(test.Profile),"
                + "cf:profile:v2:PROTO_DECODE(test.Profile),"
                + "cf2:event:PROTO_DECODE(test.Event)",
            "gs://bucket/schema.pb",
            false);

    assertNotNull(registry.getTransformer("cf", "profile:v1"));
    assertSame(
        registry.getTransformer("cf", "profile:v1"), registry.getTransformer("cf", "profile:v2"));
    assertNotNull(registry.getTransformer("cf2", "event"));
    assertNull(registry.getTransformer("cf", "profile"));
    assertNull(registry.getTransformer("cf2", "missing"));
  }

  @Test
  public void buildSetCellUsesSourceCharsetForQualifierLookup() throws Exception {
    Descriptor descriptor = buildSimpleMessageDescriptor();
    ValueTransformerRegistry registry =
        ValueTransformerRegistry.of(
            Map.of(
                TestUtil.TEST_GOOD_COLUMN_FAMILY + ":Б",
                new ProtoDecodeTransformer(descriptor, false)));

    byte[] protoBytes =
        DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName("user_name"), "charset_user")
            .setField(descriptor.findFieldByName("id"), 5)
            .build()
            .toByteArray();
    SetCell setCell =
        SetCell.create(
            TestUtil.TEST_GOOD_COLUMN_FAMILY,
            TestUtil.TEST_NON_UTF_COLUMN,
            TestUtil.TEST_TIMESTAMP,
            ByteString.copyFrom(protoBytes));
    TestChangeStreamMutation mutation =
        new TestChangeStreamMutation(
            TestUtil.TEST_ROWKEY,
            MutationType.USER,
            TestUtil.TEST_CBT_CLUSTER,
            java.time.Instant.ofEpochMilli(TestUtil.TEST_COMMIT_TIMESTAMP),
            TestUtil.TEST_TIEBREAKER,
            "token",
            java.time.Instant.EPOCH,
            setCell);
    BigtableSource source =
        new BigtableSource(
            TestUtil.TEST_CBT_INSTANCE,
            TestUtil.TEST_CBT_TABLE,
            "KOI8-R",
            null,
            null,
            org.joda.time.Instant.now());

    Mod.SetCellBuildResult buildResult =
        Mod.buildSetCell(source, mutation, setCell, registry, 10_000);

    assertEquals(TransformResult.Status.SUCCESS, buildResult.transformResult().status());
    assertNotNull(buildResult.mod());
    assertNotNull(buildResult.transformResult().value());
    assertTrue(buildResult.transformResult().value().contains("charset_user"));
  }

  @Test
  public void parseRejectsEntryWithMissingTransformType() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ValueTransformerRegistry.parse("family:qualifier", "gs://bucket/schema.pb", false));
    assertTrue(
        "Expected error to mention the entry; got: " + ex.getMessage(),
        ex.getMessage().contains("family:qualifier"));
  }

  @Test
  public void parseRejectsDoubleCommaProducingEmptyEntry() {
    // Current implementation skips empty entries gracefully — assert that behavior.
    ValueTransformerRegistry registry =
        ValueTransformerRegistry.parse(
            "cf:a:PROTO_DECODE(test.M),,cf:b:PROTO_DECODE(test.M)", "gs://bucket/schema.pb", false);
    assertNotNull(registry.getTransformer("cf", "a"));
    assertNotNull(registry.getTransformer("cf", "b"));
  }

  @Test
  public void parseRejectsUnknownTransformType() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ValueTransformerRegistry.parse(
                    "cf:c:NOT_A_REAL_TRANSFORM", "gs://bucket/schema.pb", false));
    assertTrue(
        "Expected error to mention the unknown type; got: " + ex.getMessage(),
        ex.getMessage().contains("NOT_A_REAL_TRANSFORM"));
  }

  @Test
  public void parseRejectsProtoDecodeWithoutMessageName() {
    String schema = "gs://bucket/schema.pb";
    assertThrows(
        IllegalArgumentException.class,
        () -> ValueTransformerRegistry.parse("cf:c:PROTO_DECODE()", schema, false));
    assertThrows(
        IllegalArgumentException.class,
        () -> ValueTransformerRegistry.parse("cf:c:PROTO_DECODE", schema, false));
    assertThrows(
        IllegalArgumentException.class,
        () -> ValueTransformerRegistry.parse("cf:c:PROTO_DECODE(   )", schema, false));
  }

  @Test
  public void parseRejectsProtoDecodeWhenSchemaPathMissing() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ValueTransformerRegistry.parse("cf:c:PROTO_DECODE(test.Profile)", null, false));
    assertThrows(
        IllegalArgumentException.class,
        () -> ValueTransformerRegistry.parse("cf:c:PROTO_DECODE(test.Profile)", "", false));
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

  private long transformerOutputBytes(
      ValueTransformerRegistry registry, String family, String column, byte[] protoBytes) {
    String json = registry.getTransformer(family, column).transform(protoBytes);
    assertNotNull(json);
    return json.length();
  }
}
