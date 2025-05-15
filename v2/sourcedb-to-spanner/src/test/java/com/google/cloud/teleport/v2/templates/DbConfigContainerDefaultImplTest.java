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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.IoWrapperFactory;
import com.google.cloud.teleport.v2.source.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.sdk.transforms.Wait.OnSignal;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

/** Test class for {@link DbConfigContainerDefaultImpl}. */
@RunWith(MockitoJUnitRunner.class)
public class DbConfigContainerDefaultImplTest {
  @Test
  public void testDBConfigContainerDefaultImplBasic() {
    IoWrapperFactory mockIOWrapperFactory = mock(IoWrapperFactory.class);
    IoWrapper mockIoWrapper = mock(IoWrapper.class);
    List<String> mockTables = ImmutableList.of();
    OnSignal<?> mockWaitOnSignal = mock(OnSignal.class);
    ISchemaMapper mockIschemaMapper = mock(ISchemaMapper.class);

    when(mockIOWrapperFactory.getIOWrapper(mockTables, mockWaitOnSignal)).thenReturn(mockIoWrapper);
    DbConfigContainer dbConfigContainer = new DbConfigContainerDefaultImpl(mockIOWrapperFactory);
    assertThat(dbConfigContainer.getIOWrapper(mockTables, mockWaitOnSignal))
        .isEqualTo(mockIoWrapper);
    assertThat(dbConfigContainer.getShardId()).isNull();
    assertThat(dbConfigContainer.getSrcTableToShardIdColumnMap(mockIschemaMapper, mockTables))
        .isEqualTo(new HashMap<>());
  }
}
