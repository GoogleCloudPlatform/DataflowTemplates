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
package com.google.cloud.teleport.v2.kafka.values;

public class KafkaTemplateParameters {
  public static class MessageFormatConstants {
    /**
     * Represents messages serialized in the Confluent wire format.
     *
     * <p>This format follows the Confluent wire format specification as documented in the Confluent
     * Schema Registry. Messages serialized in this format are typically associated with a schema ID
     * registered in a Schema Registry. In situations where access to the Schema Registry is
     * unavailable, messages can still be deserialized using a schema provided by the user as a
     * template parameter. The deserialization process utilizes binary encoding, as specified in the
     * Avro Binary Encoding specification.
     *
     * @see <a
     *     href="https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format">Confluent
     *     Wire Format Documentation</a>
     * @see <a href="https://avro.apache.org/docs/current/specification/#binary-encoding">Avro
     *     Binary Encoding Specification</a>
     */
    public static final String AVRO_CONFLUENT_WIRE_FORMAT = "AVRO_CONFLUENT_WIRE_FORMAT";

    /**
     * Represents messages serialized in the Avro binary format.
     *
     * <p>This format follow the Avro Binary Encoding Specification. The schema used to serialize
     * the Avro messages by the reader must be used to deserialize the bytes by the writer.
     * Otherwise, there could be unexpected errors.
     *
     * @see <a href="https://avro.apache.org/docs/current/specification/#binary-encoding">Avro
     *     Binary Encoding Specification</a>
     */
    public static final String AVRO_BINARY_ENCODING = "AVRO_BINARY_ENCODING";

    public static final String JSON = "JSON";
  }

  public static class SchemaFormat {
    public static final String SINGLE_SCHEMA_FILE = "SINGLE_SCHEMA_FILE";
    public static final String SCHEMA_REGISTRY = "SCHEMA_REGISTRY";
  }
}
