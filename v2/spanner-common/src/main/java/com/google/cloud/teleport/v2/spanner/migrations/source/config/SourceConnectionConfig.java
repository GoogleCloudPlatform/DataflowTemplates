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
package com.google.cloud.teleport.v2.spanner.migrations.source.config;

/**
 * Interface representing the connection configuration for a migration source database.
 *
 * <p>Implementing classes provide specific configuration details for different database engines
 * (e.g., Cassandra, Astra DB, JDBC/MySQL/PostgreSQL).
 *
 * <p>This interface will be expanded in Phase 2 during the platformization phase.
 */
public interface SourceConnectionConfig {}
