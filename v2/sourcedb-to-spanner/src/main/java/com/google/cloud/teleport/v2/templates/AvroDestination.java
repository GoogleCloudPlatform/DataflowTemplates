/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

@DefaultCoder(AvroCoder.class)
public class AvroDestination {
  public String name;
  public String jsonSchema;

  // Needed for serialization
  public AvroDestination() {}

  public AvroDestination(String name, String jsonSchema) {
    this.name = name;
    this.jsonSchema = jsonSchema;
  }

  public static AvroDestination of(String name, String jsonSchema) {
    return new AvroDestination(name, jsonSchema);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AvroDestination)) {
      return false;
    }
    AvroDestination that = (AvroDestination) o;
    return Objects.equals(name, that.name) && Objects.equals(jsonSchema, that.jsonSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, jsonSchema);
  }
}
