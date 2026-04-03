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

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms an 8-byte big-endian 64-bit integer representing Unix epoch milliseconds into a
 * BigQuery-compatible timestamp string.
 */
public class BigEndianTimestampTransformer implements ValueTransformer {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(BigEndianTimestampTransformer.class);
  private static final DateTimeFormatter FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneId.of("UTC"));

  @Override
  public String transform(byte[] bytes) {
    if (bytes == null || bytes.length != 8) {
      LOG.warn(
          "Expected 8 bytes for big-endian uint64 timestamp, got {}",
          bytes == null ? "null" : bytes.length);
      return null;
    }
    try {
      long millis = ByteBuffer.wrap(bytes).getLong();
      return FORMATTER.format(Instant.ofEpochMilli(millis));
    } catch (Exception e) {
      LOG.warn("Failed to decode big-endian timestamp: {}", e.getMessage());
      return null;
    }
  }
}
