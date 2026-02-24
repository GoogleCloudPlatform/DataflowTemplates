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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import com.google.auto.value.AutoValue;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import java.net.URI;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Value class for safe parsing and validation of JDBC connection strings.
 *
 * <p>It extracts key components like scheme, host, port, and database name, which are required for
 * accurate data lineage reporting. It supports various JDBC URL formats, including those for MySQL,
 * PostgreSQL, Oracle, and Derby.
 */
@AutoValue
abstract class JdbcUrl {
  private static final ImmutableSet<String> SUPPORTED_SCHEMES =
      ImmutableSet.of("mysql", "postgresql", "oracle", "derby");

  abstract String getScheme();

  abstract @Nullable String getHostAndPort();

  abstract String getDatabase();

  /**
   * Parse Jdbc Url String and return an {@link JdbcUrl} object, or return null for unsupported
   * formats.
   *
   * <p>Example of supported format:
   *
   * <ul>
   *   <li>"jdbc:postgresql://localhost:5432/postgres"
   *   <li>"jdbc:mysql://127.0.0.1:3306/db"
   *   <li>"jdbc:oracle:thin:HR/hr@localhost:5221:orcl"
   *   <li>"jdbc:derby:memory:testDB;create=true"
   *   <li>"jdbc:oracle:thin:@//myhost.example.com:1521/my_service"
   *   <li>"jdbc:mysql:///cloud_sql" (GCP CloudSQL, supported if Connection name setup via
   *       HikariDataSource)
   * </ul>
   */
  static @Nullable JdbcUrl of(String url) {
    if (Strings.isNullOrEmpty(url) || !url.startsWith("jdbc:")) {
      return null;
    }
    String[] parts = url.split(":");
    if (parts.length < 2) {
      return null;
    }
    String scheme = parts[1];
    if (!SUPPORTED_SCHEMES.contains(scheme)) {
      return null;
    }

    String cleanUri = url.substring(5);

    // 1. Resolve the scheme
    // handle sub-schemes e.g. oracle:thin (RAC)
    int start = cleanUri.indexOf("//");
    if (start != -1) {
      List<String> subschemes = Splitter.on(':').splitToList(cleanUri.substring(0, start));
      cleanUri = subschemes.get(0) + ":" + cleanUri.substring(start);
    } else {
      // not a URI format e.g. oracle:thin (non-RAC); derby in memory
      if (cleanUri.startsWith("derby:")) {
        scheme = "derby";
        int endUrl = cleanUri.indexOf(";");
        if (endUrl == -1) {
          endUrl = cleanUri.length();
        }
        List<String> components =
            Splitter.on(':').splitToList(cleanUri.substring("derby:".length(), endUrl));
        if (components.size() < 2) {
          return null;
        }
        return new AutoValue_JdbcUrl(scheme, components.get(0), components.get(1));
      } else if (cleanUri.startsWith("oracle:thin:")) {
        scheme = "oracle";

        int startHost = cleanUri.indexOf("@");
        if (startHost == -1) {
          return null;
        }
        List<String> components = Splitter.on(':').splitToList(cleanUri.substring(startHost + 1));
        if (components.size() < 3) {
          return null;
        }
        return new AutoValue_JdbcUrl(
            scheme, components.get(0) + ":" + components.get(1), components.get(2));
      } else {
        return null;
      }
    }

    URI uri = URI.create(cleanUri);
    scheme = uri.getScheme();

    // 2. resolve database
    @Nullable String path = uri.getPath();
    if (path != null && path.startsWith("/")) {
      path = path.substring(1);
    }
    if (path == null) {
      return null;
    }

    // 3. resolve host and port
    // treat as self-managed SQL instance
    @Nullable String hostAndPort = null;
    @Nullable String host = uri.getHost();
    if (host != null) {
      int port = uri.getPort();
      hostAndPort = port != -1 ? host + ":" + port : null;
    }
    return new AutoValue_JdbcUrl(scheme, hostAndPort, path);
  }
}
