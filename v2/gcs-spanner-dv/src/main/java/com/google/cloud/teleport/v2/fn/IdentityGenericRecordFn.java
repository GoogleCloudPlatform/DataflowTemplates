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
package com.google.cloud.teleport.v2.fn;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * A {@link SerializableFunction} that returns the input {@link GenericRecord} as is.
 *
 * <p>This is used to allow {@link org.apache.beam.sdk.extensions.avro.io.AvroIO} to parse
 * GenericRecords while deferring the coder inference/setting to a later stage (e.g. explicitly
 * setting {@link com.google.cloud.teleport.v2.coders.GenericRecordCoder}).
 */
public class IdentityGenericRecordFn implements SerializableFunction<GenericRecord, GenericRecord> {
  @Override
  public GenericRecord apply(GenericRecord input) {
    return input;
  }
}
