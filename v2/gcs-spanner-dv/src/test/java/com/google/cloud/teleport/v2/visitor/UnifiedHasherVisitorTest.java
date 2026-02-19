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
package com.google.cloud.teleport.v2.visitor;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Value;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.math.BigDecimal;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class UnifiedHasherVisitorTest {

  @Test
  public void testVisitString() {
    Hasher hasher = Hashing.murmur3_128().newHasher();
    UnifiedHasherVisitor visitor = new UnifiedHasherVisitor(hasher);
    String input = "test-string";

    visitor.visitString(input);
    HashCode actualHash = hasher.hash();

    HashCode expectedHash =
        Hashing.murmur3_128()
            .newHasher()
            .putByte((byte) 1)
            .putInt(input.length())
            .putString(input, UTF_8)
            .hash();

    assertEquals(expectedHash, actualHash);
  }

  @Test
  public void testVisitInt64() {
    Hasher hasher = Hashing.murmur3_128().newHasher();
    UnifiedHasherVisitor visitor = new UnifiedHasherVisitor(hasher);
    long input = 123456789L;

    visitor.visitInt64(input);
    HashCode actualHash = hasher.hash();

    HashCode expectedHash =
        Hashing.murmur3_128().newHasher().putByte((byte) 1).putLong(input).hash();

    assertEquals(expectedHash, actualHash);
  }

  @Test
  public void testVisitFloat64() {
    Hasher hasher = Hashing.murmur3_128().newHasher();
    UnifiedHasherVisitor visitor = new UnifiedHasherVisitor(hasher);
    double input = 123.456;

    visitor.visitFloat64(input);
    HashCode actualHash = hasher.hash();

    HashCode expectedHash =
        Hashing.murmur3_128().newHasher().putByte((byte) 1).putDouble(input).hash();

    assertEquals(expectedHash, actualHash);
  }

  @Test
  public void testVisitBool() {
    Hasher hasher = Hashing.murmur3_128().newHasher();
    UnifiedHasherVisitor visitor = new UnifiedHasherVisitor(hasher);
    boolean input = true;

    visitor.visitBool(input);
    HashCode actualHash = hasher.hash();

    HashCode expectedHash =
        Hashing.murmur3_128().newHasher().putByte((byte) 1).putBoolean(input).hash();

    assertEquals(expectedHash, actualHash);
  }

  @Test
  public void testVisitBytes() {
    Hasher hasher = Hashing.murmur3_128().newHasher();
    UnifiedHasherVisitor visitor = new UnifiedHasherVisitor(hasher);
    byte[] input = "test-bytes".getBytes(UTF_8);

    visitor.visitBytes(input);
    HashCode actualHash = hasher.hash();

    HashCode expectedHash =
        Hashing.murmur3_128().newHasher().putByte((byte) 1).putBytes(input).hash();

    assertEquals(expectedHash, actualHash);
  }

  @Test
  public void testVisitDate() {
    Hasher hasher = Hashing.murmur3_128().newHasher();
    UnifiedHasherVisitor visitor = new UnifiedHasherVisitor(hasher);
    Date input = Date.parseDate("2024-02-06");

    visitor.visitDate(input);
    HashCode actualHash = hasher.hash();

    HashCode expectedHash =
        Hashing.murmur3_128().newHasher().putByte((byte) 1).putString("2024-02-06", UTF_8).hash();

    assertEquals(expectedHash, actualHash);
  }

  @Test
  public void testVisitNumeric() {
    Hasher hasher = Hashing.murmur3_128().newHasher();
    UnifiedHasherVisitor visitor = new UnifiedHasherVisitor(hasher);
    BigDecimal input = new BigDecimal("123.456");

    visitor.visitNumeric(input);
    HashCode actualHash = hasher.hash();

    HashCode expectedHash =
        Hashing.murmur3_128()
            .newHasher()
            .putByte((byte) 1)
            .putString(input.toString(), UTF_8)
            .hash();

    assertEquals(expectedHash, actualHash);
  }

  @Test
  public void testVisitTimestamp() {
    Hasher hasher = Hashing.murmur3_128().newHasher();
    UnifiedHasherVisitor visitor = new UnifiedHasherVisitor(hasher);
    Timestamp input = Timestamp.parseTimestamp("2024-02-06T12:00:00Z");

    visitor.visitTimestamp(input);
    HashCode actualHash = hasher.hash();

    HashCode expectedHash =
        Hashing.murmur3_128()
            .newHasher()
            .putByte((byte) 1)
            .putString(input.toString(), UTF_8)
            .hash();

    assertEquals(expectedHash, actualHash);
  }

  @Test
  public void testVisitJson() {
    Hasher hasher = Hashing.murmur3_128().newHasher();
    UnifiedHasherVisitor visitor = new UnifiedHasherVisitor(hasher);
    String input = "{\"key\": \"value\"}";

    visitor.visitJson(input);
    HashCode actualHash = hasher.hash();

    HashCode expectedHash =
        Hashing.murmur3_128().newHasher().putByte((byte) 1).putString(input, UTF_8).hash();

    assertEquals(expectedHash, actualHash);
  }

  @Test
  public void testVisitNull() {
    Hasher hasher = Hashing.murmur3_128().newHasher();
    UnifiedHasherVisitor visitor = new UnifiedHasherVisitor(hasher);

    visitor.visitNull();
    HashCode actualHash = hasher.hash();

    HashCode expectedHash = Hashing.murmur3_128().newHasher().putByte((byte) 0).hash();

    assertEquals(expectedHash, actualHash);
  }

  @Test
  public void testVisitDefault() {
    Hasher hasher = Hashing.murmur3_128().newHasher();
    UnifiedHasherVisitor visitor = new UnifiedHasherVisitor(hasher);
    Value input = Value.string("default-value");

    visitor.visitDefault(input);
    HashCode actualHash = hasher.hash();

    HashCode expectedHash =
        Hashing.murmur3_128()
            .newHasher()
            .putByte((byte) 1)
            .putString(input.toString(), UTF_8)
            .hash();

    assertEquals(expectedHash, actualHash);
  }
}
