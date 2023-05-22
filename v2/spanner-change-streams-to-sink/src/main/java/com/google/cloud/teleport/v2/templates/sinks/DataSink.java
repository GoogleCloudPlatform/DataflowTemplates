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
package com.google.cloud.teleport.v2.templates.sinks;

import com.google.cloud.teleport.v2.templates.common.TrimmedDataChangeRecord;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/** Interface to write messages for the supported sinks. */
public interface DataSink {

  void createClient() throws IOException;

  void write(String shardId, List<TrimmedDataChangeRecord> recordsToOutput)
      throws InterruptedException, Throwable, ExecutionException;
}
