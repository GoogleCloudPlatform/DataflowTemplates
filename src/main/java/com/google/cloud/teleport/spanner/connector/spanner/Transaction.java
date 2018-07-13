/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.spanner.connector.spanner;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.BatchTransactionId;
import java.io.Serializable;
import javax.annotation.Nullable;

/** A transaction object. */
@AutoValue
public abstract class Transaction implements Serializable {

  @Nullable
  public abstract BatchTransactionId transactionId();

  public static Transaction create(BatchTransactionId txId) {
    return new AutoValue_Transaction(txId);
  }
}
