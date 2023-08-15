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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.schemautils;

import com.google.cloud.teleport.bigtable.ChangelogEntryMessage;
import com.google.cloud.teleport.bigtable.ChangelogEntryMessageProto;
import com.google.cloud.teleport.bigtable.ChangelogEntryMessageText;
import com.google.cloud.teleport.bigtable.ModType;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.BigtableSource;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.MessageEncoding;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.PubSubDestination;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.PubSubFields;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import com.google.pubsub.v1.PubsubMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.EnumMap;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.lang3.Validate;
import org.json.JSONObject;

/**
 * {@link PubSubUtils} provides utils for processing PubSub schema and generating PubSub messages.
 */
public class PubSubUtils implements Serializable {

  private static final EnumMap<PubSubFields, PubSubValueFormatter> FORMATTERS =
      new EnumMap<>(PubSubFields.class);
  private static final ThreadLocal<BinaryEncoder> BINARY_ENCODER =
      ThreadLocal.withInitial(
          () -> EncoderFactory.get().directBinaryEncoder(new ByteArrayOutputStream(), null));

  static {
    FORMATTERS.put(
        PubSubFields.ROW_KEY_STRING,
        (pb, chg) -> {
          String rowkeyEncoded = chg.getString(PubSubFields.ROW_KEY_BYTES.name());
          return pb.convertBase64ToString(rowkeyEncoded);
        });
    FORMATTERS.put(
        PubSubFields.ROW_KEY_STRING_BASE64,
        (pb, chg) -> chg.getString(PubSubFields.ROW_KEY_BYTES.name()));
    FORMATTERS.put(
        PubSubFields.ROW_KEY_BYTES,
        (pb, chg) -> {
          String rowkeyEncoded = chg.getString(PubSubFields.ROW_KEY_BYTES.name());
          return pb.convertBase64ToBytes(rowkeyEncoded);
        });
    FORMATTERS.put(PubSubFields.MOD_TYPE, (pb, chg) -> chg.getString(PubSubFields.MOD_TYPE.name()));
    FORMATTERS.put(
        PubSubFields.COMMIT_TIMESTAMP,
        (pb, chg) -> chg.getLong(PubSubFields.COMMIT_TIMESTAMP.name()));
    FORMATTERS.put(
        PubSubFields.COLUMN_FAMILY, (pb, chg) -> chg.getString(PubSubFields.COLUMN_FAMILY.name()));
    FORMATTERS.put(
        PubSubFields.COLUMN_BYTES,
        (pb, chg) -> {
          if (!chg.has(PubSubFields.COLUMN_BYTES.name())) {
            return null;
          }
          String qualifierEncoded = chg.getString(PubSubFields.COLUMN_BYTES.name());
          return pb.convertBase64ToBytes(qualifierEncoded);
        });
    FORMATTERS.put(
        PubSubFields.COLUMN_STRING,
        (pb, chg) -> {
          if (!chg.has(PubSubFields.COLUMN_BYTES.name())) {
            return null;
          }
          String qualifierEncoded = chg.getString(PubSubFields.COLUMN_BYTES.name());
          return pb.convertBase64ToString(qualifierEncoded);
        });
    FORMATTERS.put(
        (PubSubFields.COLUMN_STRING_BASE64),
        (pb, chg) -> chg.getString(PubSubFields.COLUMN_BYTES.name()));
    FORMATTERS.put(
        PubSubFields.TIMESTAMP,
        (pb, chg) -> {
          if (!chg.has(PubSubFields.TIMESTAMP.name())) {
            return null;
          }
          return chg.getLong(PubSubFields.TIMESTAMP.name());
        });
    FORMATTERS.put(
        PubSubFields.VALUE_BYTES,
        (pb, chg) -> {
          if (!chg.has(PubSubFields.VALUE_BYTES.name())) {
            return null;
          }

          String valueEncoded = chg.getString(PubSubFields.VALUE_BYTES.name());
          return pb.convertBase64ToBytes(valueEncoded);
        });
    FORMATTERS.put(
        PubSubFields.VALUE_STRING,
        (pb, chg) -> {
          if (!chg.has(PubSubFields.VALUE_BYTES.name())) {
            return null;
          }

          String valueEncoded = chg.getString(PubSubFields.VALUE_BYTES.name());
          return pb.convertBase64ToString(valueEncoded);
        });
    FORMATTERS.put(
        PubSubFields.VALUE_STRING_BASE64,
        (pb, chg) -> chg.getString(PubSubFields.VALUE_BYTES.name()));
    FORMATTERS.put(
        PubSubFields.TIMESTAMP_FROM,
        (pb, chg) -> {
          if (!chg.has(PubSubFields.TIMESTAMP_FROM.name())) {
            return null;
          }
          return chg.getLong(PubSubFields.TIMESTAMP_FROM.name());
        });
    FORMATTERS.put(
        PubSubFields.TIMESTAMP_TO,
        (pb, chg) -> {
          if (!chg.has(PubSubFields.TIMESTAMP_TO.name())) {
            return null;
          }
          return chg.getLong(PubSubFields.TIMESTAMP_TO.name());
        });
    FORMATTERS.put(PubSubFields.IS_GC, (pb, chg) -> chg.getBoolean(PubSubFields.IS_GC.name()));
    FORMATTERS.put(
        PubSubFields.SOURCE_INSTANCE,
        (pb, chg) -> chg.getString(PubSubFields.SOURCE_INSTANCE.name()));
    FORMATTERS.put(
        PubSubFields.SOURCE_CLUSTER,
        (pb, chg) -> chg.getString(PubSubFields.SOURCE_CLUSTER.name()));
    FORMATTERS.put(
        PubSubFields.SOURCE_TABLE, (pb, chg) -> chg.getString(PubSubFields.SOURCE_TABLE.name()));
    FORMATTERS.put(
        PubSubFields.TIEBREAKER, (pb, chg) -> chg.getInt(PubSubFields.TIEBREAKER.name()));

    // Just in case, validate that every column in the enum has a formatter
    for (PubSubFields field : PubSubFields.values()) {
      Validate.notNull(FORMATTERS.get(field));
    }
  }

  private final BigtableSource source;
  private final PubSubDestination destination;
  private transient Charset charsetObj;

  public PubSubUtils(BigtableSource sourceInfo, PubSubDestination destinationInfo) {
    this.source = sourceInfo;
    this.destination = destinationInfo;
    this.charsetObj = Charset.forName(sourceInfo.getCharset());
  }

  public BigtableSource getSource() {
    return source;
  }

  public PubSubDestination getDestination() {
    return destination;
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    charsetObj = Charset.forName(source.getCharset());
  }

  private String convertBase64ToString(String base64String) {
    return new String(Base64.getDecoder().decode(base64String), charsetObj);
  }

  private byte[] convertBase64ToBytes(String base64String) {
    return Base64.getDecoder().decode(base64String);
  }

  public PubsubMessage mapChangeJsonStringToPubSubMessageAsAvro(String changeJsonSting)
      throws Exception {
    MessageEncoding messageEncoding = this.getDestination().getMessageEncoding();
    ChangelogEntryMessage changelogEntryMessage = new ChangelogEntryMessage();
    JSONObject changeJsonParsed = new JSONObject(changeJsonSting);

    changelogEntryMessage.setRowKey(
        ByteBuffer.wrap(
            (byte[]) FORMATTERS.get(PubSubFields.ROW_KEY_BYTES).format(this, changeJsonParsed)));
    changelogEntryMessage.setModType(
        ModType.valueOf(
            (String) FORMATTERS.get(PubSubFields.MOD_TYPE).format(this, changeJsonParsed)));
    changelogEntryMessage.setIsGC(
        (Boolean) FORMATTERS.get(PubSubFields.IS_GC).format(this, changeJsonParsed));
    changelogEntryMessage.setTieBreaker(
        (Integer) FORMATTERS.get(PubSubFields.TIEBREAKER).format(this, changeJsonParsed));
    changelogEntryMessage.setColumnFamily(
        (String) FORMATTERS.get(PubSubFields.COLUMN_FAMILY).format(this, changeJsonParsed));
    changelogEntryMessage.setCommitTimestamp(
        (Long) FORMATTERS.get(PubSubFields.COMMIT_TIMESTAMP).format(this, changeJsonParsed));
    changelogEntryMessage.setColumn(
        ByteBuffer.wrap(
            (byte[]) FORMATTERS.get(PubSubFields.COLUMN_BYTES).format(this, changeJsonParsed)));
    changelogEntryMessage.setTimestamp(
        (Long) FORMATTERS.get(PubSubFields.TIMESTAMP).format(this, changeJsonParsed));
    changelogEntryMessage.setTimestampFrom(
        (Long) FORMATTERS.get(PubSubFields.TIMESTAMP_FROM).format(this, changeJsonParsed));
    changelogEntryMessage.setTimestampTo(
        (Long) FORMATTERS.get(PubSubFields.TIMESTAMP_TO).format(this, changeJsonParsed));
    if (destination.getStripValues()) {
      changelogEntryMessage.setValue(null);
    } else {
      changelogEntryMessage.setValue(
          ByteBuffer.wrap(
              (byte[]) FORMATTERS.get(PubSubFields.VALUE_BYTES).format(this, changeJsonParsed)));
    }
    changelogEntryMessage.setSourceInstance(
        (String) FORMATTERS.get(PubSubFields.SOURCE_INSTANCE).format(this, changeJsonParsed));
    changelogEntryMessage.setSourceCluster(
        (String) FORMATTERS.get(PubSubFields.SOURCE_CLUSTER).format(this, changeJsonParsed));
    changelogEntryMessage.setSourceTable(
        (String) FORMATTERS.get(PubSubFields.SOURCE_TABLE).format(this, changeJsonParsed));

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

    Encoder encoder = null;
    switch (messageEncoding) {
      case BINARY:
        encoder = EncoderFactory.get().directBinaryEncoder(byteStream, BINARY_ENCODER.get());
        break;
      case JSON:
        encoder =
            EncoderFactory.get().jsonEncoder(ChangelogEntryMessage.getClassSchema(), byteStream);
        break;
      default:
        throw new IllegalStateException("Unknown message encoding: " + messageEncoding);
    }

    changelogEntryMessage.customEncode(encoder);
    encoder.flush();

    // Publish the encoded object as a Pub/Sub message.
    ByteString data = ByteString.copyFrom(byteStream.toByteArray());
    return PubsubMessage.newBuilder().setData(data).build();
  }

  public PubsubMessage mapChangeJsonStringToPubSubMessageAsJson(String changeJsonSting)
      throws Exception {
    JSONObject changeJsonParsed = new JSONObject(changeJsonSting);

    var changelogEntryTextBuilder =
        ChangelogEntryMessageText.ChangelogEntryText.newBuilder()
            .setModType(
                ChangelogEntryMessageText.ChangelogEntryText.ModType.valueOf(
                    (String) FORMATTERS.get(PubSubFields.MOD_TYPE).format(this, changeJsonParsed)))
            .setIsGC((Boolean) FORMATTERS.get(PubSubFields.IS_GC).format(this, changeJsonParsed))
            .setTieBreaker(
                (Integer) FORMATTERS.get(PubSubFields.TIEBREAKER).format(this, changeJsonParsed))
            .setColumnFamily(
                (String) FORMATTERS.get(PubSubFields.COLUMN_FAMILY).format(this, changeJsonParsed))
            .setCommitTimestamp(
                (Long) FORMATTERS.get(PubSubFields.COMMIT_TIMESTAMP).format(this, changeJsonParsed))
            .setSourceInstance(
                (String)
                    FORMATTERS.get(PubSubFields.SOURCE_INSTANCE).format(this, changeJsonParsed))
            .setSourceCluster(
                (String) FORMATTERS.get(PubSubFields.SOURCE_CLUSTER).format(this, changeJsonParsed))
            .setSourceTable(
                (String) FORMATTERS.get(PubSubFields.SOURCE_TABLE).format(this, changeJsonParsed));

    if (this.destination.getUseBase64Rowkey()) {
      changelogEntryTextBuilder.setRowKey(
          (String)
              FORMATTERS.get(PubSubFields.ROW_KEY_STRING_BASE64).format(this, changeJsonParsed));
    } else {
      changelogEntryTextBuilder.setRowKey(
          (String) FORMATTERS.get(PubSubFields.ROW_KEY_STRING).format(this, changeJsonParsed));
    }

    String columnString;
    if (this.destination.getUseBase64ColumnQualifiers()) {
      columnString =
          (String) FORMATTERS.get(PubSubFields.COLUMN_STRING_BASE64).format(this, changeJsonParsed);
    } else {
      columnString =
          (String) FORMATTERS.get(PubSubFields.COLUMN_STRING).format(this, changeJsonParsed);
    }
    if (columnString != null) {
      changelogEntryTextBuilder.setColumn(columnString);
    }
    if (!destination.getStripValues()) {
      if (destination.getUseBase64Values()) {
        changelogEntryTextBuilder.setValue(
            (String)
                FORMATTERS.get(PubSubFields.VALUE_STRING_BASE64).format(this, changeJsonParsed));
      } else {
        changelogEntryTextBuilder.setValue(
            (String) FORMATTERS.get(PubSubFields.VALUE_STRING).format(this, changeJsonParsed));
      }
    }

    Long timestamp = (Long) FORMATTERS.get(PubSubFields.TIMESTAMP).format(this, changeJsonParsed);
    if (timestamp != null) {
      changelogEntryTextBuilder.setTimestamp(timestamp);
    }
    Long timestampFrom =
        (Long) FORMATTERS.get(PubSubFields.TIMESTAMP_FROM).format(this, changeJsonParsed);
    if (timestampFrom != null) {
      changelogEntryTextBuilder.setTimestampFrom(timestampFrom);
    }
    Long timestampTo =
        (Long) FORMATTERS.get(PubSubFields.TIMESTAMP_TO).format(this, changeJsonParsed);
    if (timestampTo != null) {
      changelogEntryTextBuilder.setTimestampTo(timestampTo);
    }

    ChangelogEntryMessageText.ChangelogEntryText changelogEntryMessageText =
        changelogEntryTextBuilder.build();
    PubsubMessage.Builder message = PubsubMessage.newBuilder();

    String jsonString =
        JsonFormat.printer().omittingInsignificantWhitespace().print(changelogEntryMessageText);
    message.setData(ByteString.copyFromUtf8(jsonString));
    return message.build();
  }

  public PubsubMessage mapChangeJsonStringToPubSubMessageAsProto(String changeJsonSting)
      throws Exception {
    MessageEncoding messageEncoding = this.getDestination().getMessageEncoding();
    JSONObject changeJsonParsed = new JSONObject(changeJsonSting);

    var changelogEntryProtoBuilder =
        ChangelogEntryMessageProto.ChangelogEntryProto.newBuilder()
            .setRowKey(
                ByteString.copyFrom(
                    (byte[])
                        FORMATTERS.get(PubSubFields.ROW_KEY_BYTES).format(this, changeJsonParsed)))
            .setModType(
                ChangelogEntryMessageProto.ChangelogEntryProto.ModType.valueOf(
                    (String) FORMATTERS.get(PubSubFields.MOD_TYPE).format(this, changeJsonParsed)))
            .setIsGC((Boolean) FORMATTERS.get(PubSubFields.IS_GC).format(this, changeJsonParsed))
            .setTieBreaker(
                (Integer) FORMATTERS.get(PubSubFields.TIEBREAKER).format(this, changeJsonParsed))
            .setColumnFamily(
                (String) FORMATTERS.get(PubSubFields.COLUMN_FAMILY).format(this, changeJsonParsed))
            .setCommitTimestamp(
                (Long) FORMATTERS.get(PubSubFields.COMMIT_TIMESTAMP).format(this, changeJsonParsed))
            .setSourceInstance(
                (String)
                    FORMATTERS.get(PubSubFields.SOURCE_INSTANCE).format(this, changeJsonParsed))
            .setSourceCluster(
                (String) FORMATTERS.get(PubSubFields.SOURCE_CLUSTER).format(this, changeJsonParsed))
            .setSourceTable(
                (String) FORMATTERS.get(PubSubFields.SOURCE_TABLE).format(this, changeJsonParsed));

    byte[] columnBytes =
        (byte[]) FORMATTERS.get(PubSubFields.COLUMN_BYTES).format(this, changeJsonParsed);
    if (columnBytes != null) {
      changelogEntryProtoBuilder.setColumn(ByteString.copyFrom(columnBytes));
    }
    Long timestamp = (Long) FORMATTERS.get(PubSubFields.TIMESTAMP).format(this, changeJsonParsed);
    if (timestamp != null) {
      changelogEntryProtoBuilder.setTimestamp(timestamp);
    }
    Long timestampFrom =
        (Long) FORMATTERS.get(PubSubFields.TIMESTAMP_FROM).format(this, changeJsonParsed);
    if (timestampFrom != null) {
      changelogEntryProtoBuilder.setTimestampFrom(timestampFrom);
    }
    Long timestampTo =
        (Long) FORMATTERS.get(PubSubFields.TIMESTAMP_TO).format(this, changeJsonParsed);
    if (timestampTo != null) {
      changelogEntryProtoBuilder.setTimestampTo(timestampTo);
    }
    if (!destination.getStripValues()) {
      byte[] valueBytes =
          (byte[]) FORMATTERS.get(PubSubFields.VALUE_BYTES).format(this, changeJsonParsed);
      if (valueBytes != null) {
        changelogEntryProtoBuilder.setValue(ByteString.copyFrom(valueBytes));
      }
    }

    ChangelogEntryMessageProto.ChangelogEntryProto changelogEntryMessageProto =
        changelogEntryProtoBuilder.build();
    PubsubMessage.Builder message = PubsubMessage.newBuilder();

    switch (messageEncoding) {
      case BINARY:
        message.setData(changelogEntryMessageProto.toByteString());
        break;
      case JSON:
        String jsonString =
            JsonFormat.printer()
                .omittingInsignificantWhitespace()
                .print(changelogEntryMessageProto);
        message.setData(ByteString.copyFromUtf8(jsonString));
        break;
      default:
        throw new IllegalStateException("Unknown message encoding: " + messageEncoding);
    }
    return message.build();
  }
}
