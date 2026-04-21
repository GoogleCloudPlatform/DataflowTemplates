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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.source.reader.io.IoWrapper;
import java.util.List;
import org.apache.beam.sdk.transforms.Wait;

/**
 * Container for DB Configuration. Implementations should encapsulate the relevant source
 * configuration to help with invoking sharded or non-sharded migration.
 */
public interface DbConfigContainer {

  /**
   * Create an {@link IoWrapper} instance for a list of SourceTables.
   *
   * @param sourceTables List of Source Table.
   * @param waitOnSignal Wait on previous level to complete.
   * @return ioWrapper.
   */
  IoWrapper getIOWrapper(List<String> sourceTables, Wait.OnSignal<?> waitOnSignal);
}
