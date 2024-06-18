/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstovectorsearch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test cases for the {@link Utils} class. */
@RunWith(JUnit4.class)
public class UtilsTest {

  @Test
  public void testExtractRegionFromIndexName() {
    assertEquals(
        "us-east1",
        Utils.extractRegionFromIndexName("projects/123/locations/us-east1/indexes/456"));

    var badIndexNames =
        List.of(
            "foo",
            "/projects/123/locations/us-east1/indexes",
            "projects/123/locations/us-east1/indexes/",
            "/projects/123/locations/us-east1/indexes/");

    for (var indexName : badIndexNames) {
      var ex =
          assertThrows(
              IllegalArgumentException.class,
              () -> {
                Utils.extractRegionFromIndexName(indexName);
              });
      assertEquals("Invalid IndexName", ex.getMessage());
    }
  }

  @Test
  public void testParseColumnMappings() {
    var input = "cf1:foo1->bar1,cf2:foo2->bar2";

    var got = Utils.parseColumnMapping(input);
    var want = new HashMap<String, String>();
    want.put("cf1:foo1", "bar1");
    want.put("cf2:foo2", "bar2");

    assertEquals(want, got);
  }

  @Test
  public void testParseColumnMappingWithBadInput() {
    var badMapping = "cf1:foo->bar->baz";

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              Utils.parseColumnMapping(badMapping);
            });

    assertEquals("Malformed column mapping pair cf1:foo->bar->baz", ex.getMessage());
  }

  @Test
  public void testFloatEmbeddingsAreDecoded() {
    byte[] bytes = {
      // 4 byte single precision big-endian IEEE 754 float 3.14
      (byte) 64, (byte) 72, (byte) 245, (byte) 195,
      // 4 byte single precision big-endian IEEE 754 float 2.1782
      (byte) 64, (byte) 45, (byte) 246, (byte) 253
    };
    var bs = ByteString.copyFrom(bytes);

    var want = new ArrayList<Float>(Arrays.asList(3.14f, 2.7182f));
    var got = Utils.bytesToFloats(bs, false);

    assertEquals(want, got);
  }

  @Test
  public void testEmptyEmbeddingsDecodeCorrectly() {
    byte[] bytes = {};
    var bs = ByteString.copyFrom(bytes);

    var want = new ArrayList<Float>();
    var got = Utils.bytesToFloats(bs, false);
    assertEquals(want, got);
  }

  @Test
  public void testInvalidLengthEmbeddingsProducesException() {
    byte[] bytes = {(byte) 1, (byte) 2, (byte) 3};
    var bs = ByteString.copyFrom(bytes);

    var ex =
        assertThrows(
            RuntimeException.class,
            () -> {
              Utils.bytesToFloats(bs, false);
            });

    assertEquals("Invalid ByteStream length 3 (should be a multiple of 4)", ex.getMessage());
  }

  @Test
  public void testDoubleLengthFloatEncodingsAreDecoded() {
    // Test that 8 byte doubles are correctly decoded into 4 byte floats
    byte[] bytes = {
      // 8 byte double precision big-endian IEEE 754 float 3.14
      (byte) 64, (byte) 9, (byte) 30, (byte) 184, (byte) 96, (byte) 0, (byte) 0, (byte) 0,
      // 8 byte double precision big-endian IEEE 754 float 2.1782
      (byte) 64, (byte) 5, (byte) 190, (byte) 223, (byte) 160, (byte) 0, (byte) 0, (byte) 0,
    };
    var bs = ByteString.copyFrom(bytes);

    var want = new ArrayList<Float>(Arrays.asList(3.14f, 2.7182f));
    var got = Utils.bytesToFloats(bs, true);
    assertEquals(want, got);
  }
}
