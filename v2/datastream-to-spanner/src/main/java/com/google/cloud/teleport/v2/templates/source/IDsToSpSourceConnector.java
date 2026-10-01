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
package com.google.cloud.teleport.v2.templates.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.services.datastream.v1.model.SourceConfig;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.DroppedTableException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContext;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequence;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequenceCreationException;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Interface for Datastream source connectors. Encapsulates all source-specific logic for processing
 * change events and managing sequences.
 */
public interface IDsToSpSourceConnector {

  /** Returns the unique identifier for this source type (e.g., "mysql", "postgresql", "oracle"). */
  String getSourceType();

  /** Returns true if this connector matches the given Datastream SourceConfig. */
  boolean matchesSourceConfig(SourceConfig sourceConfig);

  /** Returns the sort order mapping for shadow table columns based on the Spanner Dialect. */
  Map<String, Pair<String, String>> getSortOrder(Dialect dialect);

  /** Creates a source-specific ChangeEventContext. */
  ChangeEventContext createChangeEventContext(
      JsonNode changeEvent, Ddl ddl, Ddl shadowTableDdl, String shadowTablePrefix)
      throws ChangeEventConvertorException, InvalidChangeEventException, DroppedTableException;

  /** Creates a ChangeEventSequence from the current ChangeEventContext. */
  ChangeEventSequence createChangeEventSequenceFromChangeEventContext(
      ChangeEventContext changeEventContext)
      throws ChangeEventConvertorException, InvalidChangeEventException;

  /** Creates a ChangeEventSequence by reading from the shadow table. */
  ChangeEventSequence createChangeEventSequenceFromShadowTable(
      TransactionContext transactionContext,
      ChangeEventContext changeEventContext,
      Ddl shadowDdl,
      boolean useSqlStatements)
      throws ChangeEventSequenceCreationException, InvalidChangeEventException;
}
