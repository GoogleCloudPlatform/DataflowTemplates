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
package com.google.cloud.teleport.v2.spanner.ddl.annotations.cassandra;

import com.google.auto.value.AutoOneOf;
import com.google.cloud.teleport.v2.spanner.ddl.annotations.cassandra.CassandraType.Kind;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import org.apache.commons.lang3.StringUtils;

@AutoOneOf(Kind.class)
public abstract class CassandraType {
  public abstract Kind getKind();

  public abstract CassandraList list();

  public abstract CassandraMap map();

  public abstract String primitive();

  public abstract CassandraSet set();

  public abstract void none();

  private static Pattern listPattern = Pattern.compile("LIST<(.*?)>");
  private static Pattern mapPattern = Pattern.compile("MAP<(.*?),(.*?)>");
  private static Pattern setPattern = Pattern.compile("SET<(.*?)>");

  public static CassandraType fromAnnotation(String type) {
    String normalizedType = type.toUpperCase().replaceAll("\\s+", "");
    Matcher listMatcher = listPattern.matcher(normalizedType);
    Matcher mapMatcher = mapPattern.matcher(normalizedType);
    Matcher setMatcher = setPattern.matcher(normalizedType);
    if (StringUtils.isBlank(type)) {
      return AutoOneOf_CassandraType.none();
    }
    if (listMatcher.matches()) {
      return AutoOneOf_CassandraType.list(
          CassandraList.builder().setElementType(listMatcher.group(1)).build());
    } else if (mapMatcher.matches()) {
      return AutoOneOf_CassandraType.map(
          CassandraMap.builder()
              .setKeyType(mapMatcher.group(1))
              .setValueType(mapMatcher.group(2))
              .build());
    } else if (setMatcher.matches()) {
      return AutoOneOf_CassandraType.set(
          CassandraSet.builder().setElementType(setMatcher.group(1)).build());
    } else {
      return AutoOneOf_CassandraType.primitive(normalizedType);
    }
  }

  public enum Kind {
    NONE, /* No Cassandra Anotation */
    LIST,
    MAP,
    PRIMITIVE,
    SET
  };
}
