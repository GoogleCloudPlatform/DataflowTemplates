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
package com.google.cloud.teleport.v2.templates.dbutils.processor;

import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Represents a processor responsible for managing source database operations. This class
 * encapsulates a DML generator and a map of DAOs for interacting with source databases.
 */
public class SourceProcessor {

  private final IDMLGenerator dmlGenerator;
  private final Map<String, IDao> sourceDaoMap;

  /**
   * Constructs a SourceProcessor with the specified DML generator and source DAO map.
   *
   * @param dmlGenerator the DML generator for the source
   * @param sourceDaoMap the map of shard ID to DAO for the source
   */
  private SourceProcessor(IDMLGenerator dmlGenerator, Map<String, IDao> sourceDaoMap) {
    this.dmlGenerator = dmlGenerator;
    this.sourceDaoMap = sourceDaoMap;
  }

  public IDMLGenerator getDmlGenerator() {
    return dmlGenerator;
  }

  public Map<String, IDao> getSourceDaoMap() {
    return sourceDaoMap;
  }

  public IDao getSourceDao(String shardId) throws ConnectionException {
    return Optional.ofNullable(sourceDaoMap.get(shardId))
        .orElseThrow(
            () -> new ConnectionException("Dao object not found for shard id: " + shardId));
  }

  public void close() {
    sourceDaoMap.clear();
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder class for constructing instances of {@link SourceProcessor}. */
  public static class Builder {
    private IDMLGenerator dmlGenerator;
    private Map<String, IDao> sourceDaoMap = new HashMap<>();

    public Builder dmlGenerator(IDMLGenerator dmlGenerator) {
      this.dmlGenerator = dmlGenerator;
      return this;
    }

    public Builder sourceDaoMap(Map<String, IDao> sourceDaoMap) {
      this.sourceDaoMap = sourceDaoMap;
      return this;
    }

    public SourceProcessor build() {
      return new SourceProcessor(dmlGenerator, sourceDaoMap);
    }
  }
}
