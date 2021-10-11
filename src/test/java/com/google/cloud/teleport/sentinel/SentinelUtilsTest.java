package com.google.cloud.teleport.sentinel;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import org.junit.Test;

/** Unit tests for {@link com.google.cloud.teleport.sentinel.SentinelUtils} class. */
public class SentinelUtilsTest {

  private static final String RFC_1123_DATE = "EEE, dd MMM yyyy HH:mm:ss z";
  
  /**
   * Test whether getServerTime() format to RFC 1123 date correctly.
   */
  @Test
  public void getServerTimeTest() {

    String serverTime = SentinelUtils.getServerTime();

    assertThat(serverTime, is(containsString("GMT")));
  }

  /**
   * Test whether getHMAC256() generate the correct MAC256 hash.
   */
  @Test
  public void getHMAC256Test() throws InvalidKeyException, NoSuchAlgorithmException {
    String hash = SentinelUtils.getHMAC256("string", "key");

    assertThat(hash, is(equalTo("VMP/qL5S6lcnkSvPXZZBqqbDdM1pcDx3Ttt0ZKwPO24=")));

  }  
}
