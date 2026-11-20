/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.mongodb.validator.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Masks credentials in MongoDB connection URIs before writing to logs or reports. */
public final class UriSanitizer {

  private static final Pattern MONGO_URI_PASSWORD_PATTERN =
      Pattern.compile("(?i)^(mongodb(?:\\+srv)?://[^:@/]+:)(.+)(@[^@/]+(?:[/?].*)?)$");

  private UriSanitizer() {}

  public static String sanitize(String uri) {
    if (uri == null || uri.isEmpty()) {
      return uri;
    }
    Matcher matcher = MONGO_URI_PASSWORD_PATTERN.matcher(uri);
    if (matcher.find()) {
      return matcher.replaceFirst("$1****$3");
    }
    return uri;
  }
}
