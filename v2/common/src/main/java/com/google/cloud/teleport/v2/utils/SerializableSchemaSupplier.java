/*
 * Copyright (C) 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.teleport.v2.utils;

import java.io.IOException;
import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;

/**
 * A {@link Supplier} for {@link Schema} objects that are needed to process Avro records in
 * Beam pipelines.
 * <p>
 * Avro {@link Schema} is not serializable by default. This {@link Supplier} can be used to
 * provide a {@link Serializable} version of the schema to be used in pipelines.
 */
public class SerializableSchemaSupplier implements Serializable, Supplier<Schema> {

  /**
   * Creates a {@link Supplier} for Avro {@link Schema} that can be used in various Beam pipelines.
   *
   * @param schema Avro {@link Schema}
   * @return a {@link Supplier} that can provide the Avro schema
   */
  public static SerializableSchemaSupplier of(Schema schema) {
    return new SerializableSchemaSupplier(schema);
  }

  private final Schema schema;

  private SerializableSchemaSupplier(Schema schema) {
    this.schema = schema;
  }

  private Object writeReplace() {
    return new SerializableSchemaString(schema.toString());
  }

  @Override
  public Schema get() {
    return this.schema;
  }

  /**
   * A {@link Serializable} object that holds the {@link String} version of an Avro {@link Schema}.
   */
  private static class SerializableSchemaString implements Serializable {
    private final String schema;

    private SerializableSchemaString(String schema) {
      this.schema = schema;
    }

    private Object readResolve() throws IOException, ClassNotFoundException {
      return new SerializableSchemaSupplier(new Schema.Parser().parse(this.schema));
    }
  }
}
