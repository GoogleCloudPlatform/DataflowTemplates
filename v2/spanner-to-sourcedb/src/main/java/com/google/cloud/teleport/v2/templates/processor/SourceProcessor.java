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
package com.google.cloud.teleport.v2.templates.processor;

import com.google.cloud.teleport.v2.templates.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dml.IDMLGenerator;
import java.util.HashMap;
import java.util.Map;

public class SourceProcessor {

  private final IDMLGenerator dmlGenerator;
  private final Map<String, IDao> sourceDaoMap;

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

  public static Builder builder() {
    return new Builder();
  }

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
