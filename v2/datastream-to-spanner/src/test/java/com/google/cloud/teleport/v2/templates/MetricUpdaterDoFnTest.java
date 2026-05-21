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
package com.google.cloud.teleport.v2.templates;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Test;

public class MetricUpdaterDoFnTest {

  @Test
  public void testProcessElement_regularRunMode() {
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    FailsafeElement<String, String> failsafeElement = FailsafeElement.of("payload", "attributes");
    when(processContextMock.element()).thenReturn(failsafeElement);

    MetricUpdaterDoFn doFn = new MetricUpdaterDoFn(true);
    doFn.processElement(processContextMock);

    verify(processContextMock, times(1)).output(failsafeElement);
  }

  @Test
  public void testProcessElement_retryRunMode() {
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    FailsafeElement<String, String> failsafeElement = FailsafeElement.of("payload", "attributes");
    when(processContextMock.element()).thenReturn(failsafeElement);

    MetricUpdaterDoFn doFn = new MetricUpdaterDoFn(false);
    doFn.processElement(processContextMock);

    verify(processContextMock, times(1)).output(failsafeElement);
  }
}
