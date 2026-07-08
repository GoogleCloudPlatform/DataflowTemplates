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
package com.google.cloud.teleport.v2.templates.transforms;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UpdateDlqMetricsFnTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final MockitoRule mocktio = MockitoJUnit.rule();
  @Mock private DoFn.ProcessContext processContext;

  @Test
  public void testRegularMode() throws Exception {
    UpdateDlqMetricsFn updateDlqMetricsFn = new UpdateDlqMetricsFn(true);
    updateDlqMetricsFn.processElement(processContext);
    verify(processContext, atLeast(1)).output(any());
  }

  @Test
  public void testRetryDLQMode() throws Exception {
    UpdateDlqMetricsFn updateDlqMetricsFn = new UpdateDlqMetricsFn(false);
    updateDlqMetricsFn.processElement(processContext);
    verify(processContext, atLeast(1)).output(any());
  }
}
