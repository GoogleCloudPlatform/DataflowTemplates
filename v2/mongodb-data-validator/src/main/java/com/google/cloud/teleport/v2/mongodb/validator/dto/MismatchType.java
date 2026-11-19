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
package com.google.cloud.teleport.v2.mongodb.validator.dto;

/** Classification of validation discrepancies between source MongoDB and target Firestore. */
public enum MismatchType {
  MISSING_IN_TARGET,
  EXTRA_IN_TARGET,
  CONTENT_MISMATCH,
  MISSING_COLLECTION_IN_TARGET,
  EXTRA_COLLECTION_IN_TARGET,
  MISSING_INDEX_IN_TARGET,
  EXTRA_INDEX_IN_TARGET,
  INDEX_SPEC_MISMATCH,
  SLICE_READ_ERROR;

  public boolean isDocumentLevel() {
    return this == MISSING_IN_TARGET || this == EXTRA_IN_TARGET || this == CONTENT_MISMATCH;
  }

  public boolean isIndexLevel() {
    return this == MISSING_INDEX_IN_TARGET
        || this == EXTRA_INDEX_IN_TARGET
        || this == INDEX_SPEC_MISMATCH;
  }
}
