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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.text.StringEscapeUtils;

public class TestUtils {
  public static int wireMockResultSet(String tsvFilePath, ResultSet mockResultSet)
      throws IOException, SQLException {
    URL url = Resources.getResource(tsvFilePath);
    List<String> lines = Resources.readLines(url, Charsets.UTF_8);
    String header = lines.get(0);
    String[] columnNames = header.split("\t");
    Map<String, Integer> colToIdx = new HashedMap();
    for (int i = 0; i < columnNames.length; i++) {
      colToIdx.put(columnNames[i], i);
    }

    // Skip the header line
    lines = lines.subList(1, lines.size());

    Iterator<String> lineIterator = lines.iterator();

    AtomicReferenceArray<String> values = new AtomicReferenceArray<>(colToIdx.size());

    when(mockResultSet.next())
        .thenAnswer(
            invocation -> {
              boolean ret = lineIterator.hasNext();
              if (ret) {
                String[] valuesCur = lineIterator.next().split("\t");
                for (int i = 0; i < valuesCur.length; i++) {
                  values.set(i, valuesCur[i]);
                }
              }
              return ret;
            });

    when(mockResultSet.getString(anyString()))
        .thenAnswer(
            invocation -> {
              String colName = invocation.getArgument(0);
              return StringEscapeUtils.unescapeJava(values.get(colToIdx.get(colName)));
            });

    when(mockResultSet.getLong(anyString()))
        .thenAnswer(
            invocation -> {
              String colName = invocation.getArgument(0);
              return Long.parseLong(values.get(colToIdx.get(colName)));
            });

    when(mockResultSet.getBoolean(anyString()))
        .thenAnswer(
            invocation -> {
              String colName = invocation.getArgument(0);
              return (values.get(colToIdx.get(colName)).equals("1")
                  || Boolean.parseBoolean(values.get(colToIdx.get(colName))));
            });
    return lines.size();
  }
}
