/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.values;

import java.util.zip.Deflater;
import org.apache.avro.file.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/** Compression types supported by Dataplex mapped to Avro/Parquet codecs. */
public enum DataplexCompression {
  UNCOMPRESSED("COMPRESSION_FORMAT_UNSPECIFIED") {
    @Override
    public CodecFactory getAvroCodec() {
      return CodecFactory.nullCodec();
    }

    @Override
    public CompressionCodecName getParquetCodec() {
      return CompressionCodecName.UNCOMPRESSED;
    }
  },
  // TODO: Update Dataplex compression name once Snappy is supported.
  SNAPPY("COMPRESSION_FORMAT_UNSPECIFIED" /* Not supported yet */) {
    @Override
    public CodecFactory getAvroCodec() {
      return CodecFactory.snappyCodec();
    }

    @Override
    public CompressionCodecName getParquetCodec() {
      return CompressionCodecName.SNAPPY;
    }
  },
  GZIP("GZIP") {
    @Override
    public CodecFactory getAvroCodec() {
      return CodecFactory.deflateCodec(Deflater.DEFAULT_COMPRESSION);
    }

    @Override
    public CompressionCodecName getParquetCodec() {
      return CompressionCodecName.UNCOMPRESSED;
    }
  },
  BZIP2("BZIP2") {
    @Override
    public CodecFactory getAvroCodec() {
      return CodecFactory.bzip2Codec();
    }

    @Override
    public CompressionCodecName getParquetCodec() {
      throw new UnsupportedOperationException("BZIP2 compression not supported for Parquet files");
    }
  };

  private final String dataplexCompressionName;

  public abstract CodecFactory getAvroCodec();

  public abstract CompressionCodecName getParquetCodec();

  DataplexCompression(String dataplexCompressionName) {
    this.dataplexCompressionName = dataplexCompressionName;
  }

  public String getDataplexCompressionName() {
    return dataplexCompressionName;
  }
}
