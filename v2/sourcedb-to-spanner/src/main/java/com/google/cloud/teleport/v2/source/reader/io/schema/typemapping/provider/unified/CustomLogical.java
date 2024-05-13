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
package com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/**
 * Wraps Constants for Custom logical types needed by <a
 * href=https://cloud.google.com/datastream/docs/unified-types>unified types</a>.
 */
public final class CustomLogical {
  public static final class Json {
    public static final String LOGICAL_TYPE = "json";
    public static final Schema SCHEMA =
        new LogicalType(LOGICAL_TYPE).addToSchema(SchemaBuilder.builder().stringType());

    /** Static final class wrapping only constants. * */
    private Json() {}
  }

  public static final class Number {
    public static final String LOGICAL_TYPE = "number";
    public static final Schema SCHEMA =
        new LogicalType(LOGICAL_TYPE).addToSchema(SchemaBuilder.builder().stringType());

    /** Static final class wrapping only constants. * */
    private Number() {}
  }

  public static class TimeIntervalMicros {
    public static final String LOGICAL_TYPE = "time-interval-micros";
    public static final Schema SCHEMA =
        new LogicalType(LOGICAL_TYPE).addToSchema(SchemaBuilder.builder().longType());

    /** Static final class wrapping only constants. * */
    private TimeIntervalMicros() {}
  }

  /** Static final class wrapping only constants. * */
  private CustomLogical() {}
}
