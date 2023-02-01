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
package com.google.cloud.teleport.v2.templates.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;

/**
 * TODO: move this somewhere nice Lifted from the below.
 * https://github.com/apache/beam/blob/master/sdks/java/io/hbase/src/main/java/org/apache/beam/sdk/io/hbase/HBaseMutationCoder.java
 */
public class RowMutationsCoder extends AtomicCoder<RowMutations> implements Serializable {
  // Code stub for use in registrar.
  // https://github.com/apache/beam/blob/master/sdks/java/io/hbase/src/main/java/org/apache/beam/sdk/io/hbase/HBaseCoderProviderRegistrar.java
  private static final RowMutationsCoder INSTANCE = new RowMutationsCoder();

  RowMutationsCoder() {}

  public static RowMutationsCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(RowMutations value, OutputStream outStream)
      throws CoderException, IOException {
    // TODO: figure out neater way to encode byte[] rowKey
    byte[] rowKey = value.getRow();
    int rowKeyLen = rowKey.length;

    // serialize row key
    outStream.write(rowKeyLen);
    outStream.write(rowKey);

    // serialize mutation list
    List<Mutation> mutations = value.getMutations();
    int mutationsSize = mutations.size();
    outStream.write(mutationsSize);
    for (Mutation mutation : mutations) {
      MutationType type = getType(mutation);
      MutationProto proto = ProtobufUtil.toMutation(type, mutation);
      proto.writeDelimitedTo(outStream);
    }
  }

  private static MutationType getType(Mutation mutation) {
    if (mutation instanceof Put) {
      return MutationType.PUT;
    } else if (mutation instanceof Delete) {
      return MutationType.DELETE;
    } else {
      throw new IllegalArgumentException("Only Put and Delete are supported");
    }
  }

  @Override
  public RowMutations decode(InputStream inStream) throws IOException {

    int rowKeyLen = inStream.read();
    byte[] rowKey = inStream.readNBytes(rowKeyLen);

    RowMutations rowMutations = new RowMutations(rowKey);
    int mutationListSize = inStream.read();
    for (int i = 0; i < mutationListSize; i++) {
      Mutation m = ProtobufUtil.toMutation(MutationProto.parseDelimitedFrom(inStream));
      MutationType type = getType(m);

      // TODO: verify if force casting works here
      if (type == MutationType.PUT) {
        rowMutations.add((Put) m);
      } else if (type == MutationType.DELETE) {
        rowMutations.add((Delete) m);
      }
    }
    return rowMutations;
  }
}
