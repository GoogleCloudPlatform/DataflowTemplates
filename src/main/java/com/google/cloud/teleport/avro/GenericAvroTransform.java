package com.google.cloud.teleport.avro;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The focus of this class is:
 * <ul>
 *     <li>Getter and cache of schemas</li>
 *     <li>Translate PubSub events to AVRO GenericRecords</li>
 * </ul>
 *
 * Two external sources are supported:
 * <ul>
 *     <li>GCS Bucket</li>
 *     <li>HTTP server</li>
 * </ul>
 *
 * On each case, will concat the schema global id to url of the external resource to get it.
 */
public class GenericAvroTransform implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(GenericAvroTransform.class);

  private final HashMap<Integer, String> schemaMap;
  private final ValueProvider<String> schemaProviderUri;

  public GenericAvroTransform(String path) {
    this(ValueProvider.StaticValueProvider.of(path));
  }

  public GenericAvroTransform(ValueProvider<String> pathProvider) {
    schemaProviderUri = pathProvider;
    schemaMap = new HashMap<>();
  }

  public GenericRecord convert(AvroPubsubMessageRecord element) {
    try {
      Integer globalId = schemaRegistryId(element);
      Schema itemSchema = schemaFrom(globalId);
      DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(itemSchema);
      byte[] recordPayload = recordPayloadFrom(element);
      Decoder decoder = DecoderFactory.get().binaryDecoder(recordPayload, null);
      return datumReader.read(null, decoder);
    } catch (IOException ex) {
      throw new RuntimeException("Converting Pubsub event to GenericRecord", ex);
    }
  }

  public Integer schemaRegistryId(AvroPubsubMessageRecord element) {
    return ByteBuffer.wrap(Arrays.copyOfRange(element.getMessage(), 1, 5)).getInt();
  }

  public Schema schemaFrom(AvroPubsubMessageRecord element) {
    return schemaFrom(schemaRegistryId(element));
  }

  public Schema schemaFrom(Integer globalId) {
    String schemaStr = schemaMap.computeIfAbsent(globalId, this::schemaStringFromUri);
    return new Schema.Parser().parse(schemaStr);
  }

  private byte[] recordPayloadFrom(AvroPubsubMessageRecord element) {
    return Arrays.copyOfRange(element.getMessage(), 5, element.getMessage().length);
  }

  private String schemaStringFromUri(Integer globalId) {
    String url = schemaProviderUri.get() + globalId;
    if (url.toLowerCase().startsWith("gs://")) {
      return schemaStringFromBucket(url);
    } else {
      return schemaStringFromRest(url);
    }
  }

  private String schemaStringFromBucket(String bucketUrl) {
    ResourceId resourceId = null;
    try {
      MatchResult match = FileSystems.match(bucketUrl);
      if (match.metadata().isEmpty()) {
        LOG.warn("Schema not found as {}. Return null", bucketUrl);
        return null;
      }
      resourceId = match.metadata().get(0).resourceId();
    } catch (IOException ex) {
      LOG.error("Trying to get schema from {}. Return null", bucketUrl, ex);
      return null;
    }
    try (ReadableByteChannel channel = FileSystems.open(resourceId);
       InputStream inputStream = Channels.newInputStream(channel)) {
      return stringFromInputStream(inputStream);
    } catch (IOException ex) {
      LOG.error("Trying to read schema from {}. Return null", bucketUrl, ex);
      return null;
    }
  }

  private String schemaStringFromRest(String restUrl) {
    HttpURLConnection con = null;
    try {
      URL url = new URL(restUrl);
      con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("GET");
      con.setInstanceFollowRedirects(true);
      int status = con.getResponseCode();
      if (status != 200) {
        throw new FileNotFoundException(restUrl);
      }
      return stringFromInputStream(con.getInputStream());
    } catch (IOException ex) {
      LOG.error("Trying to get schema from {}. Return null", restUrl, ex);
      return null;
    } finally {
      if (con != null) { con.disconnect(); }
    }
  }

  private String stringFromInputStream(InputStream inputStream) {
    try (InputStreamReader isr = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
       BufferedReader br = new BufferedReader(isr);) {
      return br.lines().collect(Collectors.joining("\n"));
    } catch (IOException ex) {
      LOG.error("Trying to get avro schema content: {}. Return null", ex.getMessage(), ex);
      return null;
    }
  }
}
