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
package com.google.cloud.teleport.v2.neo4j.database;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;

public final class Neo4jCapabilities implements Serializable {

  private final String versionString;
  private final Neo4jVersion version;
  private final Neo4jEdition edition;

  public Neo4jCapabilities(String version, String edition) {
    this.version = Neo4jVersion.of(version);
    this.edition = Neo4jEdition.of(version, edition);
    this.versionString = String.format("Neo4j %s %s", version, edition);
  }

  public Neo4jEdition edition() {
    return edition;
  }

  public boolean hasVectorIndexes() {
    return edition != Neo4jEdition.COMMUNITY && version.compareTo(Neo4jVersion.V5_13_0) >= 0;
  }

  public boolean hasNodeTypeConstraints() {
    return edition != Neo4jEdition.COMMUNITY && version.compareTo(Neo4jVersion.V5_11_0) >= 0;
  }

  public boolean hasRelationshipTypeConstraints() {
    return edition != Neo4jEdition.COMMUNITY && version.compareTo(Neo4jVersion.V5_11_0) >= 0;
  }

  public boolean hasNodeKeyConstraints() {
    return edition != Neo4jEdition.COMMUNITY;
  }

  public boolean hasRelationshipKeyConstraints() {
    return edition != Neo4jEdition.COMMUNITY && version.compareTo(Neo4jVersion.V5_7_0) >= 0;
  }

  public boolean hasNodeUniqueConstraints() {
    return true;
  }

  public boolean hasRelationshipUniqueConstraints() {
    return version.compareTo(Neo4jVersion.V5_7_0) >= 0;
  }

  public boolean hasNodeExistenceConstraints() {
    return edition != Neo4jEdition.COMMUNITY;
  }

  public boolean hasRelationshipExistenceConstraints() {
    return edition != Neo4jEdition.COMMUNITY;
  }

  public boolean hasCreateOrReplaceDatabase() {
    return edition == Neo4jEdition.ENTERPRISE;
  }

  @Override
  public String toString() {
    return versionString;
  }

  enum Neo4jEdition {
    COMMUNITY,
    ENTERPRISE,
    AURA;

    public static Neo4jEdition of(String version, String edition) {
      if (version.toLowerCase(Locale.ROOT).endsWith("-aura")) {
        return AURA;
      }

      return Neo4jEdition.valueOf(edition.toUpperCase(Locale.ROOT));
    }
  }

  static class Neo4jVersion implements Comparable<Neo4jVersion> {

    public static final Neo4jVersion V5_7_0 = new Neo4jVersion(5, 7, 0);
    public static final Neo4jVersion V5_11_0 = new Neo4jVersion(5, 11, 0);
    public static final Neo4jVersion V5_13_0 = new Neo4jVersion(5, 13, 0);

    private final int major;
    private final int minor;
    private final int patch;

    Neo4jVersion(int major, int minor) {
      this(major, minor, Integer.MAX_VALUE);
    }

    Neo4jVersion(int major, int minor, int patch) {
      this.major = major;
      this.minor = minor;
      this.patch = patch;
    }

    public static Neo4jVersion of(String version) {
      int major = -1;
      int minor = -1;
      int patch = -1;
      String buffer = "";
      for (char c : version.toCharArray()) {
        if (c != '.') {
          buffer += c;
          continue;
        }
        if (major == -1) {
          major = Integer.parseInt(buffer, 10);
        } else if (minor == -1) {
          minor = parseMinor(buffer);
        } else {
          throw invalidVersion(version);
        }
        buffer = "";
      }
      if (buffer.isEmpty()) {
        throw invalidVersion(version);
      }
      if (minor == -1) {
        minor = parseMinor(buffer);
      } else {
        patch = parsePatch(buffer);
      }

      if (major == -1 || minor == -1) {
        throw invalidVersion(version);
      }
      if (patch == -1) {
        return new Neo4jVersion(major, minor);
      }
      return new Neo4jVersion(major, minor, patch);
    }

    @Override
    public int compareTo(Neo4jVersion other) {
      if (major != other.major) {
        return signum(major - other.major);
      }
      if (minor != other.minor) {
        return signum(minor - other.minor);
      }
      return signum(patch - other.patch);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Neo4jVersion that)) {
        return false;
      }
      return major == that.major && minor == that.minor && patch == that.patch;
    }

    @Override
    public int hashCode() {
      return Objects.hash(major, minor, patch);
    }

    @Override
    public String toString() {
      if (patch == Integer.MAX_VALUE) {
        return String.format("%d.%d", major, minor);
      }
      return String.format("%d.%d.%d", major, minor, patch);
    }

    private static int parseMinor(String buffer) {
      return Integer.parseInt(buffer.replace("-aura", ""), 10);
    }

    private static int parsePatch(String buffer) {
      int end = buffer.indexOf('-');
      if (end == -1) {
        end = buffer.length();
      }
      return Integer.parseInt(buffer.substring(0, end), 10);
    }

    private static int signum(int result) {
      return (int) Math.signum(result);
    }

    private static IllegalArgumentException invalidVersion(String version) {
      return new IllegalArgumentException(String.format("Invalid Neo4j version: %s", version));
    }
  }
}
