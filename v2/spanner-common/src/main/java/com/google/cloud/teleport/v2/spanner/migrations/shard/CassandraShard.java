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
package com.google.cloud.teleport.v2.spanner.migrations.shard;

import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import java.util.List;
import java.util.Objects;

public class CassandraShard extends Shard {
  private final OptionsMap optionsMap;

  public CassandraShard(OptionsMap optionsMap) {
    super();
    this.optionsMap = optionsMap;
    validateFields();
    extractAndSetHostAndPort();
  }

  private void validateFields() {
    if (getContactPoints() == null || getContactPoints().isEmpty()) {
      throw new IllegalArgumentException("CONTACT_POINTS cannot be null or empty.");
    }
    if (getKeySpaceName() == null || getKeySpaceName().isEmpty()) {
      throw new IllegalArgumentException("SESSION_KEYSPACE cannot be null or empty.");
    }
  }

  private void extractAndSetHostAndPort() {
    String firstContactPoint = getContactPoints().get(0);
    String[] parts = firstContactPoint.split(":");

    if (parts.length < 2) {
      throw new IllegalArgumentException("Invalid contact point format: " + firstContactPoint);
    }

    String host = parts[0];
    String port = parts[1];

    setHost(host);
    setPort(port);
  }

  private String getOptionValue(TypedDriverOption<String> key) {
    return optionsMap.get(key);
  }

  private List<String> getOptionValueList(TypedDriverOption<List<String>> key) {
    return optionsMap.get(key);
  }

  // Getters that fetch data from OptionsMap
  public List<String> getContactPoints() {
    return getOptionValueList(TypedDriverOption.CONTACT_POINTS);
  }

  public String getKeySpaceName() {
    return getOptionValue(TypedDriverOption.SESSION_KEYSPACE);
  }

  public String getUsername() {
    return getOptionValue(TypedDriverOption.AUTH_PROVIDER_USER_NAME);
  }

  public String getPassword() {
    return getOptionValue(TypedDriverOption.AUTH_PROVIDER_PASSWORD);
  }

  public OptionsMap getOptionsMap() {
    return this.optionsMap;
  }

  @Override
  public String toString() {
    return String.format(
        "CassandraShard{logicalShardId='%s', contactPoints=%s, keyspace='%s', host='%s', port='%s'}",
        getLogicalShardId(), getContactPoints(), getKeySpaceName(), getHost(), getPort());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CassandraShard)) {
      return false;
    }
    CassandraShard that = (CassandraShard) o;
    return Objects.equals(getContactPoints(), that.getContactPoints())
        && Objects.equals(getKeySpaceName(), that.getKeySpaceName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getContactPoints(), getKeySpaceName());
  }
}
