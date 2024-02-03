/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.constants;

/** A single class to store all constants. */
public class Constants {

  /** Run mode - regular. */
  public static final String RUN_MODE_REGULAR = "regular";

  /** Run mode - reprocess. */
  public static final String RUN_MODE_REPROCESS = "reprocess";

  /** Run mode - resumeSuccess. */
  public static final String RUN_MODE_RESUME_SUCCESS = "resumeSuccess";

  /** Run mode - resumeFailed. */
  public static final String RUN_MODE_RESUME_FAILED = "resumeFailed";

  /** Run mode - resumeAll. */
  public static final String RUN_MODE_RESUME_ALL = "resumeAll";

  /** Shard progress status - success. */
  public static final String SHARD_PROGRESS_STATUS_SUCCESS = "SUCCESS";

  /** Shard progress status - error. */
  public static final String SHARD_PROGRESS_STATUS_ERROR = "ERROR";

  /** Shard progress status - reprocess. */
  public static final String SHARD_PROGRESS_STATUS_REPROCESS = "REPROCESS";
}
