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
package com.google.cloud.teleport.v2.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.bigtable.v2.Mutation;
import com.google.cloud.teleport.v2.transforms.StreamingDataGeneratorWriteToBigtable.BytesToNativeMutationFn;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

/** Unit tests for {@link StreamingDataGeneratorWriteToBigtable}. */
@RunWith(JUnit4.class)
public class StreamingDataGeneratorWriteToBigtableTest {

  @Test
  public void testBytesToNativeMutationFn() throws Exception {
    BytesToNativeMutationFn fn = new BytesToNativeMutationFn("cf", "id");
    fn.setup();

    String json = "{\"id\":\"row1\",\"name\":\"Alice\",\"age\":30}";
    byte[] message = json.getBytes(StandardCharsets.UTF_8);

    @SuppressWarnings("unchecked")
    OutputReceiver<KV<ByteString, Iterable<Mutation>>> receiver = mock(OutputReceiver.class);

    fn.processElement(message, receiver);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<KV<ByteString, Iterable<Mutation>>> captor = ArgumentCaptor.forClass(KV.class);

    verify(receiver).output(captor.capture());

    KV<ByteString, Iterable<Mutation>> kv = captor.getValue();
    assertEquals("row1", kv.getKey().toStringUtf8());

    Iterator<Mutation> iter = kv.getValue().iterator();

    assertTrue(iter.hasNext());
    Mutation mut1 = iter.next();
    assertTrue(mut1.hasSetCell());
    assertEquals("cf", mut1.getSetCell().getFamilyName());
    assertEquals("name", mut1.getSetCell().getColumnQualifier().toStringUtf8());
    assertEquals("Alice", mut1.getSetCell().getValue().toStringUtf8());

    assertTrue(iter.hasNext());
    Mutation mut2 = iter.next();
    assertTrue(mut2.hasSetCell());
    assertEquals("cf", mut2.getSetCell().getFamilyName());
    assertEquals("age", mut2.getSetCell().getColumnQualifier().toStringUtf8());
    assertEquals("30", mut2.getSetCell().getValue().toStringUtf8());
  }
}
