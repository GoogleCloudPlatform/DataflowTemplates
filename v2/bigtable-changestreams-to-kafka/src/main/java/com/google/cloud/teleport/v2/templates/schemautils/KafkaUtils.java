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
package com.google.cloud.teleport.v2.templates.schemautils;

import com.google.cloud.teleport.bigtable.ChangelogEntryMessage;
import com.google.cloud.teleport.bigtable.ChangelogEntryMessageBase64;
import com.google.cloud.teleport.bigtable.ModType;
import com.google.cloud.teleport.v2.templates.model.KafkaFields;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.EnumMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

/** {@link KafkaUtils} provides utils for processing Kafka schema and generating Kafka messages. */
public class KafkaUtils implements Serializable {
  private static final EnumMap<KafkaFields, KafkaValueFormatter> FORMATTERS =
      new EnumMap<>(KafkaFields.class);

  static {
    FORMATTERS.put(
        KafkaFields.ROW_KEY,
        (k, chg) -> {
          String rowkeyEncoded = chg.getString(KafkaFields.ROW_KEY.name());
          return convertBase64ToBytes(rowkeyEncoded);
        });
    FORMATTERS.put(KafkaFields.MOD_TYPE, (k, chg) -> chg.getString(KafkaFields.MOD_TYPE.name()));
    FORMATTERS.put(
        KafkaFields.COMMIT_TIMESTAMP, (k, chg) -> chg.getLong(KafkaFields.COMMIT_TIMESTAMP.name()));
    FORMATTERS.put(
        KafkaFields.COLUMN_FAMILY, (k, chg) -> chg.getString(KafkaFields.COLUMN_FAMILY.name()));
    FORMATTERS.put(
        KafkaFields.COLUMN_BYTES,
        (k, chg) -> {
          if (!chg.has(KafkaFields.COLUMN_BYTES.name())) {
            return null;
          }
          String qualifierEncoded = chg.getString(KafkaFields.COLUMN_BYTES.name());
          return convertBase64ToBytes(qualifierEncoded);
        });
    FORMATTERS.put(
        KafkaFields.COLUMN_STRING,
        (k, chg) -> {
          if (!chg.has(KafkaFields.COLUMN_BYTES.name())) {
            return null;
          }
          String qualifierEncoded = chg.getString(KafkaFields.COLUMN_BYTES.name());
          return k.convertBase64ToString(qualifierEncoded);
        });
    FORMATTERS.put(
        KafkaFields.TIMESTAMP,
        (k, chg) -> {
          if (!chg.has(KafkaFields.TIMESTAMP.name())) {
            return null;
          }
          return chg.getLong(KafkaFields.TIMESTAMP.name());
        });
    FORMATTERS.put(
        KafkaFields.VALUE_BYTES,
        (k, chg) -> {
          if (!chg.has(KafkaFields.VALUE_BYTES.name())) {
            return null;
          }

          String valueEncoded = chg.getString(KafkaFields.VALUE_BYTES.name());
          return convertBase64ToBytes(valueEncoded);
        });
    FORMATTERS.put(
        KafkaFields.VALUE_STRING,
        (k, chg) -> {
          if (!chg.has(KafkaFields.VALUE_BYTES.name())) {
            return null;
          }

          String valueEncoded = chg.getString(KafkaFields.VALUE_BYTES.name());
          return k.convertBase64ToString(valueEncoded);
        });
    FORMATTERS.put(
        KafkaFields.TIMESTAMP_FROM,
        (k, chg) -> {
          if (!chg.has(KafkaFields.TIMESTAMP_FROM.name())) {
            return null;
          }
          return chg.getLong(KafkaFields.TIMESTAMP_FROM.name());
        });
    FORMATTERS.put(
        KafkaFields.TIMESTAMP_TO,
        (k, chg) -> {
          if (!chg.has(KafkaFields.TIMESTAMP_TO.name())) {
            return null;
          }
          return chg.getLong(KafkaFields.TIMESTAMP_TO.name());
        });
    FORMATTERS.put(KafkaFields.IS_GC, (k, chg) -> chg.getBoolean(KafkaFields.IS_GC.name()));
    FORMATTERS.put(
        KafkaFields.SOURCE_INSTANCE, (k, chg) -> chg.getString(KafkaFields.SOURCE_INSTANCE.name()));
    FORMATTERS.put(
        KafkaFields.SOURCE_CLUSTER, (k, chg) -> chg.getString(KafkaFields.SOURCE_CLUSTER.name()));
    FORMATTERS.put(
        KafkaFields.SOURCE_TABLE, (k, chg) -> chg.getString(KafkaFields.SOURCE_TABLE.name()));
    FORMATTERS.put(KafkaFields.TIEBREAKER, (k, chg) -> chg.getInt(KafkaFields.TIEBREAKER.name()));

    // Just in case, validate that every column in the enum has a formatter
    for (KafkaFields field : KafkaFields.values()) {
      Validate.notNull(
          FORMATTERS.get(field), "Formatter for field '" + field.name() + "' not set up.");
    }
  }

  private static byte[] convertBase64ToBytes(String base64String) {
    return Base64.getDecoder().decode(base64String);
  }

  private final String charset;
  private transient Charset charsetObj;

  public KafkaUtils(String charset) {
    this.charset = charset;
    this.charsetObj = Charset.forName(charset);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    charsetObj = Charset.forName(charset);
  }

  private String convertBase64ToString(String base64String) {
    return new String(Base64.getDecoder().decode(base64String), charsetObj);
  }

  public ProducerRecord<byte[], GenericRecord> getProducerRecord(
      String changeJsonSting, String destinationTopic) throws Exception {
    ChangelogEntryMessage changelogEntryMessage = new ChangelogEntryMessage();
    JSONObject changeJsonParsed = new JSONObject(changeJsonSting);

    changelogEntryMessage.setRowKey(
        ByteBuffer.wrap(
            (byte[]) FORMATTERS.get(KafkaFields.ROW_KEY).format(this, changeJsonParsed)));
    changelogEntryMessage.setModType(
        ModType.valueOf(
            (String) FORMATTERS.get(KafkaFields.MOD_TYPE).format(this, changeJsonParsed)));
    changelogEntryMessage.setIsGC(
        (Boolean) FORMATTERS.get(KafkaFields.IS_GC).format(this, changeJsonParsed));
    changelogEntryMessage.setTieBreaker(
        (Integer) FORMATTERS.get(KafkaFields.TIEBREAKER).format(this, changeJsonParsed));
    changelogEntryMessage.setColumnFamily(
        (String) FORMATTERS.get(KafkaFields.COLUMN_FAMILY).format(this, changeJsonParsed));
    changelogEntryMessage.setCommitTimestamp(
        (Long) FORMATTERS.get(KafkaFields.COMMIT_TIMESTAMP).format(this, changeJsonParsed));
    changelogEntryMessage.setColumn(
        ByteBuffer.wrap(
            (byte[]) FORMATTERS.get(KafkaFields.COLUMN_BYTES).format(this, changeJsonParsed)));
    changelogEntryMessage.setTimestamp(
        (Long) FORMATTERS.get(KafkaFields.TIMESTAMP).format(this, changeJsonParsed));
    changelogEntryMessage.setTimestampFrom(
        (Long) FORMATTERS.get(KafkaFields.TIMESTAMP_FROM).format(this, changeJsonParsed));
    changelogEntryMessage.setTimestampTo(
        (Long) FORMATTERS.get(KafkaFields.TIMESTAMP_TO).format(this, changeJsonParsed));
    changelogEntryMessage.setValue(
        ByteBuffer.wrap(
            (byte[]) FORMATTERS.get(KafkaFields.VALUE_BYTES).format(this, changeJsonParsed)));

    changelogEntryMessage.setSourceInstance(
        (String) FORMATTERS.get(KafkaFields.SOURCE_INSTANCE).format(this, changeJsonParsed));
    changelogEntryMessage.setSourceCluster(
        (String) FORMATTERS.get(KafkaFields.SOURCE_CLUSTER).format(this, changeJsonParsed));
    changelogEntryMessage.setSourceTable(
        (String) FORMATTERS.get(KafkaFields.SOURCE_TABLE).format(this, changeJsonParsed));

    return new ProducerRecord<>(
        destinationTopic, changelogEntryMessage.getRowKey().array(), changelogEntryMessage);
  }

  public ProducerRecord<byte[], GenericRecord> getProducerRecordWithBase64EncodedFields(
      String changeJsonSting, String destinationTopic) throws Exception {
    ChangelogEntryMessageBase64 changelogEntryMessage = new ChangelogEntryMessageBase64();
    JSONObject changeJsonParsed = new JSONObject(changeJsonSting);
    Base64.Encoder b64 = Base64.getEncoder();

    changelogEntryMessage.setRowKey(
        b64.encodeToString(
            (byte[]) FORMATTERS.get(KafkaFields.ROW_KEY).format(this, changeJsonParsed)));
    changelogEntryMessage.setModType(
        ModType.valueOf(
            (String) FORMATTERS.get(KafkaFields.MOD_TYPE).format(this, changeJsonParsed)));
    changelogEntryMessage.setIsGC(
        (Boolean) FORMATTERS.get(KafkaFields.IS_GC).format(this, changeJsonParsed));
    changelogEntryMessage.setTieBreaker(
        (Integer) FORMATTERS.get(KafkaFields.TIEBREAKER).format(this, changeJsonParsed));
    changelogEntryMessage.setColumnFamily(
        (String) FORMATTERS.get(KafkaFields.COLUMN_FAMILY).format(this, changeJsonParsed));
    changelogEntryMessage.setCommitTimestamp(
        (Long) FORMATTERS.get(KafkaFields.COMMIT_TIMESTAMP).format(this, changeJsonParsed));
    changelogEntryMessage.setColumn(
        b64.encodeToString(
            (byte[]) FORMATTERS.get(KafkaFields.COLUMN_BYTES).format(this, changeJsonParsed)));
    changelogEntryMessage.setTimestamp(
        (Long) FORMATTERS.get(KafkaFields.TIMESTAMP).format(this, changeJsonParsed));
    changelogEntryMessage.setTimestampFrom(
        (Long) FORMATTERS.get(KafkaFields.TIMESTAMP_FROM).format(this, changeJsonParsed));
    changelogEntryMessage.setTimestampTo(
        (Long) FORMATTERS.get(KafkaFields.TIMESTAMP_TO).format(this, changeJsonParsed));
    changelogEntryMessage.setValue(
        b64.encodeToString(
            (byte[]) FORMATTERS.get(KafkaFields.VALUE_BYTES).format(this, changeJsonParsed)));

    changelogEntryMessage.setSourceInstance(
        (String) FORMATTERS.get(KafkaFields.SOURCE_INSTANCE).format(this, changeJsonParsed));
    changelogEntryMessage.setSourceCluster(
        (String) FORMATTERS.get(KafkaFields.SOURCE_CLUSTER).format(this, changeJsonParsed));
    changelogEntryMessage.setSourceTable(
        (String) FORMATTERS.get(KafkaFields.SOURCE_TABLE).format(this, changeJsonParsed));

    return new ProducerRecord<>(
        destinationTopic,
        Base64.getDecoder().decode(changelogEntryMessage.getRowKey().toString()),
        changelogEntryMessage);
  }
}
