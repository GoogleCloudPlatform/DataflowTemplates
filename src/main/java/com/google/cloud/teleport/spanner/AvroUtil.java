/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.spanner;

import java.util.List;
import org.apache.avro.Schema;

/** Avro utilities. */
public class AvroUtil {
  private AvroUtil() {}

  public static Schema unpackNullable(Schema schema) {
    if (schema.getType() != Schema.Type.UNION) {
      return null;
    }
    List<Schema> unionTypes = schema.getTypes();
    if (unionTypes.size() != 2) {
      return null;
    }

    if (unionTypes.get(0).getType() == Schema.Type.NULL) {
      return unionTypes.get(1);
    }
    if (unionTypes.get(1).getType() == Schema.Type.NULL) {
      return unionTypes.get(0);
    }
    return null;
  }
}
