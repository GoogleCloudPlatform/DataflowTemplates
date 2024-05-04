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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstovectorsearch;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

public class Utils {

  public static String extractRegionFromIndexName(String indexName) {
    Pattern p = Pattern.compile("^?projects\\/\\d+/locations\\/([^/]+)\\/indexes/\\d+?$");
    Matcher matcher = p.matcher(indexName);
    if (matcher.find()) {
      String region = matcher.group(1);
      return region;
    }

    throw new IllegalArgumentException("Invalid IndexName");
  }

  // Split "cf1:foo1->bar1,cf1:foo2->bar2" into a map of { "cf1:foo1": "bar1", "cf1:foo2": "bar2" }
  public static Map<String, String> parseColumnMapping(String mapstr) {
    Map<String, String> columnsWithAliases = new HashMap<>();
    if (StringUtils.isBlank(mapstr)) {
      return columnsWithAliases;
    }
    String[] columnsList = mapstr.split(",");

    for (String columnsWithAlias : columnsList) {
      String[] columnWithAlias = columnsWithAlias.split("->");
      if (columnWithAlias.length == 2
          && columnWithAlias[0].length() >= 1
          && columnWithAlias[1].length() >= 1) {
        columnsWithAliases.put(columnWithAlias[0], columnWithAlias[1]);
      } else {
        throw new IllegalArgumentException(
            String.format("Malformed column mapping pair %s", columnsList));
      }
    }
    return columnsWithAliases;
  }

  // Convert a ByteString into an array of 4 byte single-precision floats
  // If parseDoubles is true, interpret the ByteString as containing 8 byte double-precision floats,
  // which are narrowed to 4 byte floats
  // If parseDoubles is false, interpret the ByteString as containing 4 byte floats
  public static ArrayList<Float> bytesToFloats(ByteString value, Boolean parseDoubles) {
    byte[] bytes = value.toByteArray();
    int bytesPerFloat = (parseDoubles ? 8 : 4);

    if (bytes.length % bytesPerFloat != 0) {
      throw new RuntimeException(
          String.format(
              "Invalid ByteStream length %d (should be a multiple of %d)",
              bytes.length, bytesPerFloat));
    }

    var embeddings = new ArrayList<Float>();
    for (int i = 0; i < bytes.length; i += bytesPerFloat) {
      if (parseDoubles) {
        embeddings.add((float) Bytes.toDouble(bytes, i));
      } else {
        embeddings.add(Bytes.toFloat(bytes, i));
      }
    }

    return embeddings;
  }
}
