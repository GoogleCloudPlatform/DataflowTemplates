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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper;

import com.google.common.collect.ImmutableMap;
import java.io.Serializable;

/**
 * An interface to be implemented for various jdbc source types to get the {@link JdbcValueMapper}
 * for various source types.
 */
public interface JdbcValueMappingsProvider extends Serializable {

  /**
   * Get Mapping of source types to {@link JdbcValueMapper}.
   *
   * @return mapping.
   */
  ImmutableMap<String, JdbcValueMapper<?>> getMappings();
}
