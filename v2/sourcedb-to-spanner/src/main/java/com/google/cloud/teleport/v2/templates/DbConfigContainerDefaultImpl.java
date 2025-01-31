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

import com.google.cloud.teleport.v2.source.reader.IoWrapperFactory;
import com.google.cloud.teleport.v2.source.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.Wait.OnSignal;
import org.jetbrains.annotations.Nullable;

/** Default Implementation for {@link DbConfigContainer} for non-Sharded sources. */
public final class DbConfigContainerDefaultImpl implements DbConfigContainer {
  private IoWrapperFactory ioWrapperFactory;

  public DbConfigContainerDefaultImpl(IoWrapperFactory ioWrapperFactory) {
    this.ioWrapperFactory = ioWrapperFactory;
  }

  /** Create an {@link IoWrapper} instance for a list of SourceTables. */
  @Override
  public IoWrapper getIOWrapper(List<String> sourceTables, OnSignal<?> waitOnSignal) {
    return this.ioWrapperFactory.getIOWrapper(sourceTables, waitOnSignal);
  }

  @Nullable
  @Override
  public String getShardId() {
    return null;
  }

  @Override
  public Map<String, String> getSrcTableToShardIdColumnMap(
      ISchemaMapper schemaMapper, List<String> spannerTables) {
    return new HashMap<>();
  }
}
