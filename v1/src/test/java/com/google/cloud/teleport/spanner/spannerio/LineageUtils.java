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
package com.google.cloud.teleport.spanner.spannerio;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Utils class for getting FQ names and parts using {@link Lineage}. */
public class LineageUtils {

  /**
   * Builds a fully qualified name (FQN) string from the system and its segments. Old method for the
   * compatibility.
   */
  static String getFqName(String system, Iterable<String> segments) {
    StringBuilder sb = new StringBuilder();
    for (String segment : getFQNParts(system, null, segments, null)) {
      sb.append(segment);
    }
    return sb.toString();
  }

  /**
   * Constructs an iterable of string parts representing a fully qualified name (FQN), based on the
   * provided system, optional subtype, hierarchical segments, and an optional separator to be
   * appended after the final segment. See {@link Lineage} class for details.
   */
  static Iterable<String> getFQNParts(
      String system,
      @Nullable String subtype,
      Iterable<String> segments,
      @Nullable String lastSegmentSep) {

    List<String> parts = new ArrayList<>();
    parts.add(system + ":");
    if (subtype != null) {
      parts.add(subtype + ":");
    }

    if (segments != null) {
      Iterator<String> iterator = segments.iterator();
      String previousSegment = null;
      while (iterator.hasNext()) {
        if (previousSegment != null) {
          parts.add(Lineage.wrapSegment(previousSegment) + ".");
        }
        previousSegment = iterator.next();
      }

      if (previousSegment != null) {
        if (lastSegmentSep != null) {
          List<String> subSegments =
              Splitter.onPattern(lastSegmentSep).splitToList(Lineage.wrapSegment(previousSegment));
          for (int i = 0; i < subSegments.size() - 1; i++) {
            parts.add(subSegments.get(i) + lastSegmentSep);
          }
          parts.add(subSegments.get(subSegments.size() - 1));
        } else {
          parts.add(Lineage.wrapSegment(previousSegment));
        }
      }
    }
    return parts;
  }
}
