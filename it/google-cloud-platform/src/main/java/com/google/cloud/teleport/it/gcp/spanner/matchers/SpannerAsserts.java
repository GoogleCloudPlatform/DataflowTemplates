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
package com.google.cloud.teleport.it.gcp.spanner.matchers;

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatRecords;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.it.common.matchers.RecordsSubject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SpannerAsserts {

  /**
   * Convert Spanner {@link Struct} list to a list of maps.
   *
   * @param structs Structs to parse
   * @return List of maps to use in {@link RecordsSubject}
   */
  public static List<Map<String, Object>> structsToRecords(List<Struct> structs) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (Struct struct : structs) {
        Map<String, Object> record = new HashMap<>();

        for (Type.StructField field : struct.getType().getStructFields()) {
          Value fieldValue = struct.getValue(field.getName());
          // May need to explore using typed methods instead of .toString()
          record.put(field.getName(), fieldValue.toString());
        }

        records.add(record);
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting TableResult to Records", e);
    }
  }

  /**
   * Creates a {@link RecordsSubject} to assert information within a list of records.
   *
   * @param structs Records in Spanner {@link Struct} format to use in the comparison.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatStructs(List<Struct> structs) {
    return assertThatRecords(structsToRecords(structs));
  }
}
