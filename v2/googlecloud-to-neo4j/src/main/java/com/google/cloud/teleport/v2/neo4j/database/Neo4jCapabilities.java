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

public final class Neo4jCapabilities implements Serializable {

  private final String versionString;
  private final Neo4jVersion version;
  private final Neo4jEdition edition;

  public Neo4jCapabilities(String version, String edition) {
    this.version = Neo4jVersion.of(version);
    this.edition = Neo4jEdition.of(version, edition);
    this.versionString = String.format("Neo4j %s %s", version, edition);
  }

  public boolean hasConstraints() {
    return edition != Neo4jEdition.COMMUNITY;
  }

  public boolean hasNodeKeyConstraints() {
    return hasConstraints();
  }

  public boolean hasNodeUniqueConstraints() {
    return hasConstraints();
  }

  public boolean hasRelationshipKeyConstraints() {
    return hasConstraints() && version == Neo4jVersion.V5;
  }

  public boolean hasRelationshipUniqueConstraints() {
    return hasRelationshipKeyConstraints();
  }

  public boolean hasNodeExistenceConstraints() {
    return hasConstraints();
  }

  public boolean hasRelationshipExistenceConstraints() {
    return hasConstraints();
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

  enum Neo4jVersion {
    UNKNOWN,
    V4_4,
    V5;

    public static Neo4jVersion of(String version) {
      if (version.startsWith("4.4")) {
        return V4_4;
      } else if (version.startsWith("5.")) {
        return V5;
      } else {
        return UNKNOWN;
      }
    }
  }
}
