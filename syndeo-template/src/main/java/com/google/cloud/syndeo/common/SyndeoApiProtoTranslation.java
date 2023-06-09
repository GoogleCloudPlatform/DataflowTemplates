/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.syndeo.common;

import com.google.cloud.syndeo.v1.SyndeoV1;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.model.pipeline.v1.SchemaApi;

public class SyndeoApiProtoTranslation {

  public static SyndeoV1.Schema toSyndeoProtos(SchemaApi.Schema schema) {
    // TODO(laraschmidt): Proper translation.
    try {
      return SyndeoV1.Schema.parseFrom(schema.toByteArray());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Unable to parse proto");
    }
  }

  public static SchemaApi.Schema fromSyndeoProtos(SyndeoV1.Schema schema) {
    // TODO(laraschmidt): Proper translation.
    try {
      return SchemaApi.Schema.parseFrom(schema.toByteArray());
    } catch (
        org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.InvalidProtocolBufferException e) {
      throw new RuntimeException("Unable to parse proto");
    }
  }

  public static SyndeoV1.Row toSyndeoProtos(SchemaApi.Row row) {
    // TODO(laraschmidt): Proper translation.
    try {
      return SyndeoV1.Row.parseFrom(row.toByteArray());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Unable to parse proto");
    }
  }

  public static SchemaApi.Row fromSyndeoProtos(SyndeoV1.Row row) {
    // TODO(laraschmidt): Proper translation.
    try {
      return SchemaApi.Row.parseFrom(row.toByteArray());
    } catch (
        org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.InvalidProtocolBufferException e) {
      throw new RuntimeException("Unable to parse proto");
    }
  }
}
