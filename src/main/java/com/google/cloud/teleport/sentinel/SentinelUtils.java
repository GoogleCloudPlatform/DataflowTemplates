package com.google.cloud.teleport.sentinel;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * A class utility for Sentinel Http end point.
 */
public class SentinelUtils {

  private static final String RFC_1123_DATE = "EEE, dd MMM yyyy HH:mm:ss z";
  
  public static String getServerTime() {
      Calendar calendar = Calendar.getInstance();
      SimpleDateFormat dateFormat = new SimpleDateFormat(RFC_1123_DATE, Locale.US);
      dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
      return dateFormat.format(calendar.getTime());
  }

  public static String getHMAC256(String input, String key) throws InvalidKeyException, NoSuchAlgorithmException {
    String hash;
    Mac sha256HMAC = Mac.getInstance("HmacSHA256");
    Base64.Decoder decoder = Base64.getDecoder();
    SecretKeySpec secretKey = new SecretKeySpec(decoder.decode(key.getBytes(StandardCharsets.UTF_8)), "HmacSHA256");
    sha256HMAC.init(secretKey);
    Base64.Encoder encoder = Base64.getEncoder();
    hash = new String(encoder.encode(sha256HMAC.doFinal(input.getBytes(StandardCharsets.UTF_8))));
    return hash;
  }  
}
