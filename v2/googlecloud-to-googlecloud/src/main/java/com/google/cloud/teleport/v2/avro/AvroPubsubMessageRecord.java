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
package com.google.cloud.teleport.v2.avro;

import com.google.common.base.MoreObjects;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

/**
 * The {@link AvroPubsubMessageRecord} class is an Avro wrapper class for {@link
 * org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage} which captures the message's fields along with
 * the event timestamp for archival purposes.
 */
@DefaultCoder(AvroCoder.class)
public class AvroPubsubMessageRecord {

  private byte[] message;
  private Map<String, String> attributes;
  private long timestamp;

  // Private empty constructor used for reflection required by AvroIO.
  @SuppressWarnings("unused")
  private AvroPubsubMessageRecord() {}

  public AvroPubsubMessageRecord(byte[] message, Map<String, String> attributes, long timestamp) {
    this.message = message;
    this.attributes = attributes;
    this.timestamp = timestamp;
  }

  public byte[] getMessage() {
    return this.message;
  }

  public void setMessage(byte[] message) {
    this.message = message;
  }

  public Map<String, String> getAttributes() {
    return this.attributes;
  }

  public void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object obj) {

    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final AvroPubsubMessageRecord other = (AvroPubsubMessageRecord) obj;

    return Objects.deepEquals(this.getMessage(), other.getMessage())
        && Objects.equals(this.getAttributes(), other.getAttributes())
        && Objects.equals(this.getTimestamp(), other.getTimestamp());
  }

  @Override
  public int hashCode() {
    return Objects.hash(Arrays.hashCode(message), attributes, timestamp);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("message", message)
        .add("attributes", attributes)
        .add("timestamp", timestamp)
        .toString();
  }
}
