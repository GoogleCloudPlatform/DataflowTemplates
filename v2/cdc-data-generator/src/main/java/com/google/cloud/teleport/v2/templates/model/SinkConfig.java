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
package com.google.cloud.teleport.v2.templates.model;

import java.io.Serializable;

/**
 * Interface representing a parsed, serializable sink configuration.
 *
 * <p>Architectural Note on {@link Serializable}: This interface extends {@code Serializable}
 * because instances are stored as fields within Apache Beam {@code DoFn}s and {@code PTransform}s.
 * Beam uses Java serialization exactly once during job submission and worker initialization to
 * distribute the execution graph. Once initialized on worker nodes, configuration fields are
 * accessed as warm in-memory references, incurring zero per-record serialization overhead during
 * active data processing.
 */
public interface SinkConfig extends Serializable {}
