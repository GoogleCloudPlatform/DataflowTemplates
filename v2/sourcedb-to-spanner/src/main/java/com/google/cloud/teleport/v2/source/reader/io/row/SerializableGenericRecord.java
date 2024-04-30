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
package com.google.cloud.teleport.v2.source.reader.io.row;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

/**
 * Wrap {@link GenericRecord} into a serializable type.
 *
 * <p>Unlike, {@link org.apache.avro.specific.SpecificRecord}, {@code GenericRecord}s do not have a
 * schema at compile time. Hence, a genericRecord needs a custom serde that encodes both the schema
 * and the data.
 *
 * <p><b>Note:</b>
 *
 * <ol>
 *   <li>Based on benchmarking results, this can be further enhanced to have a singleton schema
 *       registry and encode only the hash of the schema in the serialization.
 *   <li>This can't be made as an {@link com.google.auto.value.AutoValue} class, as they are not
 *       mutable and do not support custom serializations.
 * </ol>
 */
// TODO(vardhanvthigle): Explore using a coder instead and avoid having an extra encapsulating
// class.
class SerializableGenericRecord implements Serializable {
  private GenericRecord record;

  /**
   * Construct {@link SerializableGenericRecord}.
   *
   * @param record the enclosed avro generic record
   */
  public SerializableGenericRecord(GenericRecord record) {
    this.record = record;
  }

  /**
   * Getter for the enclosed generic record.
   *
   * @return enclosed generic record
   */
  public GenericRecord getRecord() {
    return this.record;
  }

  private void setRecord(GenericRecord record) {
    this.record = record;
  }

  /**
   * Serializes the enclosed {@link GenericRecord} by using {@link GenericDatumWriter}. This method
   * is required to be implemented by the {@link Serializable} interface.
   *
   * @param out output stream.
   * @throws IOException exception due to any problem with OutputStream.
   */
  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    out.writeObject(this.getRecord().getSchema());
    // TODO: Check if reuse can help with performance.
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<>(this.getRecord().getSchema()).write(this.getRecord(), encoder);
    encoder.flush();
  }

  /**
   * Deserializes the enclosed {@link GenericRecord} by using {@link GenericDatumReader}. This
   * method is required to be implemented by the {@link Serializable} interface.
   *
   * @param in Input stream.
   * @throws IOException Exception for any problem with Input stream.
   * @throws ClassNotFoundException Corrupted input, probably missing schema.
   */
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    Schema schema = (Schema) in.readObject();
    // TODO: Check if reuse can help with performance.
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
    GenericRecord record = (new GenericDatumReader<GenericRecord>(schema)).read(null, decoder);
    this.setRecord(record);
  }

  /**
   * This method is required by {@link Serializable} interface to initialize the object if needed.
   * This is an empty method for our use case and will not be covered by UT.
   *
   * @throws ObjectStreamException Exception with Object stream.
   */
  private void readObjectNoData() throws ObjectStreamException {}

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SerializableGenericRecord)) {
      return false;
    }
    SerializableGenericRecord that = (SerializableGenericRecord) o;
    return getRecord().equals(that.getRecord());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getRecord());
  }
}
