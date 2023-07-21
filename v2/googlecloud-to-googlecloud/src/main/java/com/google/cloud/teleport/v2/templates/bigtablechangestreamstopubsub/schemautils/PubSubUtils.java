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

import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.teleport.bigtable.ChangeLogEntry;
import com.google.cloud.teleport.bigtable.ChangeLogEntryJson;
import com.google.cloud.teleport.bigtable.ChangeLogEntryProto;
import com.google.cloud.teleport.bigtable.ModType;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.BigtableSource;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.PubSubDestination;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.PubSubFields;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import com.google.pubsub.v1.PubsubMessage;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.EnumMap;
import org.apache.avro.io.*;
import org.apache.commons.lang3.Validate;
import org.json.JSONObject;

/**
 * {@link PubSubUtils} provides utils for processing PubSub schema and generating PubSub messages.
 */
public class PubSubUtils implements Serializable {

  private static final EnumMap<PubSubFields, PubSubValueFormatter> FORMATTERS =
      new EnumMap<>(PubSubFields.class);

  static {
    FORMATTERS.put(
        PubSubFields.ROW_KEY_STRING,
        (pb, chg) -> {
          String rowkeyEncoded = chg.getString(PubSubFields.ROW_KEY_BYTES.name());
          return pb.convertBase64ToString(rowkeyEncoded);
        });
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
        PubSubFields.COLUMN,
        (pb, chg) -> {
          if (!chg.has(PubSubFields.COLUMN.name())) {
            return null;
          }
          String qualifierEncoded = chg.getString(PubSubFields.COLUMN.name());
          return pb.convertBase64ToBytes(qualifierEncoded);
        });
    FORMATTERS.put(
        PubSubFields.TIMESTAMP,
        (pb, chg) -> {
          if (!chg.has(PubSubFields.TIMESTAMP.name())) {
            return null;
          }
          return chg.getString(PubSubFields.TIMESTAMP.name());
        });
    FORMATTERS.put(
        PubSubFields.TIMESTAMP_NUM,
        (pb, chg) -> {
          if (!chg.has(PubSubFields.TIMESTAMP_NUM.name())) {
            return null;
          }
          return chg.getLong(PubSubFields.TIMESTAMP_NUM.name());
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
        PubSubFields.VALUE_BYTES,
        (pb, chg) -> {
          if (!chg.has(PubSubFields.VALUE_BYTES.name())) {
            return null;
          }

          String valueEncoded = chg.getString(PubSubFields.VALUE_BYTES.name());
          return pb.convertBase64ToBytes(valueEncoded);
        });
    FORMATTERS.put(
        PubSubFields.TIMESTAMP_FROM,
        (pb, chg) -> {
          if (!chg.has(PubSubFields.TIMESTAMP_FROM.name())) {
            return null;
          }
          return chg.getString(PubSubFields.TIMESTAMP_FROM.name());
        });
    FORMATTERS.put(
        PubSubFields.TIMESTAMP_FROM_NUM,
        (pb, chg) -> {
          if (!chg.has(PubSubFields.TIMESTAMP_FROM_NUM.name())) {
            return null;
          }
          return chg.getLong(PubSubFields.TIMESTAMP_FROM_NUM.name());
        });
    FORMATTERS.put(
        PubSubFields.TIMESTAMP_TO,
        (pb, chg) -> {
          if (!chg.has(PubSubFields.TIMESTAMP_TO.name())) {
            return null;
          }
          return chg.getString(PubSubFields.TIMESTAMP_TO.name());
        });
    FORMATTERS.put(
        PubSubFields.TIMESTAMP_TO_NUM,
        (pb, chg) -> {
          if (!chg.has(PubSubFields.TIMESTAMP_TO_NUM.name())) {
            return null;
          }
          return chg.getLong(PubSubFields.TIMESTAMP_TO_NUM.name());
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

  private String convertBase64ToString(String base64String) {
    return new String(Base64.getDecoder().decode(base64String), charsetObj);
  }

  private byte[] convertBase64ToBytes(String base64String) {
    return Base64.getDecoder().decode(base64String);
  }

  public PubsubMessage mapChangeJsonStringToPubSubMessageAsAvro(String changeJsonSting)
      throws Exception {
    String messageEncoding = this.getDestination().getMessageEncoding();
    ChangeLogEntry changelogEntry = new ChangeLogEntry();
    try {
      JSONObject changeJsonParsed = new JSONObject(changeJsonSting);

      changelogEntry.setRowKey(
          ByteBuffer.wrap(
              (byte[]) FORMATTERS.get(PubSubFields.ROW_KEY_BYTES).format(this, changeJsonParsed)));
      changelogEntry.setModType(
          ModType.valueOf(
              (String) FORMATTERS.get(PubSubFields.MOD_TYPE).format(this, changeJsonParsed)));
      changelogEntry.setIsGc(
          (Boolean) FORMATTERS.get(PubSubFields.IS_GC).format(this, changeJsonParsed));
      changelogEntry.setTieBreaker(
          (Integer) FORMATTERS.get(PubSubFields.TIEBREAKER).format(this, changeJsonParsed));
      changelogEntry.setColumnFamily(
          (String) FORMATTERS.get(PubSubFields.COLUMN_FAMILY).format(this, changeJsonParsed));
      changelogEntry.setCommitTimestamp(
          (Long) FORMATTERS.get(PubSubFields.COMMIT_TIMESTAMP).format(this, changeJsonParsed));
      changelogEntry.setColumn(
          ByteBuffer.wrap(
              (byte[]) FORMATTERS.get(PubSubFields.COLUMN).format(this, changeJsonParsed)));
      changelogEntry.setTimestamp(
          (Long) FORMATTERS.get(PubSubFields.TIMESTAMP_NUM).format(this, changeJsonParsed));
      changelogEntry.setTimestampFrom(
          (Long) FORMATTERS.get(PubSubFields.TIMESTAMP_FROM_NUM).format(this, changeJsonParsed));
      changelogEntry.setTimestampTo(
          (Long) FORMATTERS.get(PubSubFields.TIMESTAMP_TO_NUM).format(this, changeJsonParsed));
      changelogEntry.setValue(
          ByteBuffer.wrap(
              (byte[]) FORMATTERS.get(PubSubFields.VALUE_BYTES).format(this, changeJsonParsed)));

    } catch (Exception e) {
      throw e;
    }
    Publisher publisher = null;
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

    Encoder encoder = null;
    switch (messageEncoding) {
      case "BINARY":
        encoder = EncoderFactory.get().directBinaryEncoder(byteStream, /* reuse= */ null);
        break;
      case "JSON":
        encoder = EncoderFactory.get().jsonEncoder(ChangeLogEntry.getClassSchema(), byteStream);
        break;
      default:
        final String errorMessage =
            "Invalid message encoding:"
                + messageEncoding
                + ". Supported output formats: JSON, BINARY";
        throw new IllegalArgumentException(errorMessage);
    }
    changelogEntry.customEncode(encoder);
    encoder.flush();

    // Publish the encoded object as a Pub/Sub message.
    ByteString data = ByteString.copyFrom(byteStream.toByteArray());
    PubsubMessage message = PubsubMessage.newBuilder().setData(data).build();
    return message;
  }

  private String bytesToString(ByteBuffer bytes, boolean useBase64, Charset charset) {
    if (bytes == null) {
      return null;
    } else {
      if (useBase64) {
        return charset.decode(Base64.getEncoder().encode(bytes)).toString();
      } else {
        return charset.decode(bytes).toString();
      }
    }
  }

  public PubsubMessage mapChangeJsonStringToPubSubMessageAsJson(String changeJsonSting)
      throws Exception {
    String messageEncoding = this.getDestination().getMessageEncoding();
    ChangeLogEntryJson changeLogEntryJson = new ChangeLogEntryJson();
    try {
      JSONObject changeJsonParsed = new JSONObject(changeJsonSting);

      changeLogEntryJson.setRowKey(
          bytesToString(
              ByteBuffer.wrap(
                  (byte[])
                      FORMATTERS.get(PubSubFields.ROW_KEY_STRING).format(this, changeJsonParsed)),
              this.destination.getUseBase64Rowkey(),
              this.charsetObj));
      changeLogEntryJson.setModType(
          ModType.valueOf(
              (String) FORMATTERS.get(PubSubFields.MOD_TYPE).format(this, changeJsonParsed)));
      changeLogEntryJson.setIsGc(
          (Boolean) FORMATTERS.get(PubSubFields.IS_GC).format(this, changeJsonParsed));
      changeLogEntryJson.setTieBreaker(
          (Integer) FORMATTERS.get(PubSubFields.TIEBREAKER).format(this, changeJsonParsed));
      changeLogEntryJson.setColumnFamily(
          (String) FORMATTERS.get(PubSubFields.COLUMN_FAMILY).format(this, changeJsonParsed));
      changeLogEntryJson.setCommitTimestamp(
          (Long) FORMATTERS.get(PubSubFields.COMMIT_TIMESTAMP).format(this, changeJsonParsed));
      changeLogEntryJson.setColumn(
          bytesToString(
              ByteBuffer.wrap(
                  (byte[]) FORMATTERS.get(PubSubFields.COLUMN).format(this, changeJsonParsed)),
              this.destination.getUseBase64ColumnQualifier(),
              this.charsetObj));
      changeLogEntryJson.setTimestamp(
          (Long) FORMATTERS.get(PubSubFields.TIMESTAMP_NUM).format(this, changeJsonParsed));
      changeLogEntryJson.setTimestampFrom(
          (Long) FORMATTERS.get(PubSubFields.TIMESTAMP_FROM_NUM).format(this, changeJsonParsed));
      changeLogEntryJson.setTimestampTo(
          (Long) FORMATTERS.get(PubSubFields.TIMESTAMP_TO_NUM).format(this, changeJsonParsed));
      changeLogEntryJson.setValue(
          bytesToString(
              ByteBuffer.wrap(
                  (byte[]) FORMATTERS.get(PubSubFields.VALUE_BYTES).format(this, changeJsonParsed)),
              this.destination.getUseBase64Value(),
              this.charsetObj));

    } catch (Exception e) {
      throw e;
    }

    Publisher publisher = null;
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

    Encoder encoder = null;
    switch (messageEncoding) {
      case "BINARY":
        encoder = EncoderFactory.get().directBinaryEncoder(byteStream, /* reuse= */ null);
        break;
      case "JSON":
        encoder = EncoderFactory.get().jsonEncoder(ChangeLogEntryJson.getClassSchema(), byteStream);
        break;
      default:
        final String errorMessage =
            "Invalid message encoding:"
                + messageEncoding
                + ". Supported output formats: JSON, BINARY";
        throw new IllegalArgumentException(errorMessage);
    }
    changeLogEntryJson.customEncode(encoder);
    encoder.flush();

    // Publish the encoded object as a Pub/Sub message.
    ByteString data = ByteString.copyFrom(byteStream.toByteArray());
    PubsubMessage message = PubsubMessage.newBuilder().setData(data).build();
    return message;
  }

  public PubsubMessage mapChangeJsonStringToPubSubMessageAsProto(String changeJsonSting)
      throws Exception {
    String messageEncoding = this.getDestination().getMessageEncoding();
    ChangeLogEntryProto.ChangelogEntryProto changeLogEntryProto;

    try {
      JSONObject changeJsonParsed = new JSONObject(changeJsonSting);
      changeLogEntryProto =
          ChangeLogEntryProto.ChangelogEntryProto.newBuilder()
              .setRowKey(
                  ByteString.copyFrom(
                      (byte[])
                          FORMATTERS
                              .get(PubSubFields.ROW_KEY_BYTES)
                              .format(this, changeJsonParsed)))
              .setModType(
                  ChangeLogEntryProto.ChangelogEntryProto.ModType.valueOf(
                      (String)
                          FORMATTERS.get(PubSubFields.MOD_TYPE).format(this, changeJsonParsed)))
              .setIsGC((Boolean) FORMATTERS.get(PubSubFields.IS_GC).format(this, changeJsonParsed))
              .setTieBreaker(
                  (Integer) FORMATTERS.get(PubSubFields.TIEBREAKER).format(this, changeJsonParsed))
              .setColumnFamily(
                  (String)
                      FORMATTERS.get(PubSubFields.COLUMN_FAMILY).format(this, changeJsonParsed))
              .setCommitTimestamp(
                  (Long)
                      FORMATTERS.get(PubSubFields.COMMIT_TIMESTAMP).format(this, changeJsonParsed))
              .setColumn(
                  ByteString.copyFrom(
                      (byte[]) FORMATTERS.get(PubSubFields.COLUMN).format(this, changeJsonParsed)))
              .setTimestamp(
                  (Long) FORMATTERS.get(PubSubFields.TIMESTAMP_NUM).format(this, changeJsonParsed))
              .setTimestampFrom(
                  (Long)
                      FORMATTERS
                          .get(PubSubFields.TIMESTAMP_FROM_NUM)
                          .format(this, changeJsonParsed))
              .setTimestampTo(
                  (Long)
                      FORMATTERS.get(PubSubFields.TIMESTAMP_TO_NUM).format(this, changeJsonParsed))
              .setValue(
                  ByteString.copyFrom(
                      (byte[])
                          FORMATTERS.get(PubSubFields.VALUE_BYTES).format(this, changeJsonParsed)))
              .build();

    } catch (Exception e) {
      throw e;
    }

    Publisher publisher = null;
    PubsubMessage.Builder message = PubsubMessage.newBuilder();

    switch (messageEncoding) {
      case "BINARY":
        message.setData(changeLogEntryProto.toByteString());
        break;
      case "JSON":
        String jsonString =
            JsonFormat.printer().omittingInsignificantWhitespace().print(changeLogEntryProto);
        message.setData(ByteString.copyFromUtf8(jsonString));
        break;
      default:
        final String errorMessage =
            "Invalid message encoding:"
                + messageEncoding
                + ". Supported output formats: JSON, BINARY";
        throw new IllegalArgumentException(errorMessage);
    }
    return message.build();
  }
}
