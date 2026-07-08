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
package com.google.cloud.teleport.v2.spanner.migrations.utils;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.EVENT_METADATA_KEY_PREFIX;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ChangeEventUtils {
  public static List<String> getEventColumnKeys(JsonNode changeEvent)
      throws InvalidChangeEventException {
    // Filter all keys which have the metadata prefix
    Iterator<String> fieldNames = changeEvent.fieldNames();
    List<String> eventColumnKeys =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(fieldNames, Spliterator.ORDERED), false)
            .filter(f -> !f.startsWith(EVENT_METADATA_KEY_PREFIX))
            .collect(Collectors.toList());
    if (eventColumnKeys.size() == 0) {
      throw new InvalidChangeEventException("No data found in Datastream event. ");
    }
    return eventColumnKeys;
  }
}
