/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.spanner.spannerio.changestreams.util;

import com.google.cloud.Timestamp;
import com.google.cloud.teleport.spanner.spannerio.changestreams.dao.PartitionMetadataDao.InTransactionContext;
import com.google.cloud.teleport.spanner.spannerio.changestreams.dao.PartitionMetadataDao.TransactionResult;
import java.util.function.Function;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestTransactionAnswer implements Answer<TransactionResult<Object>> {

  private final InTransactionContext transaction;

  public TestTransactionAnswer(InTransactionContext transaction) {
    this.transaction = transaction;
  }

  @Override
  public TransactionResult<Object> answer(InvocationOnMock invocation) {
    Function<InTransactionContext, Object> callable = invocation.getArgument(0);
    final Object result = callable.apply(transaction);
    return new TransactionResult<>(result, Timestamp.now());
  }
}
