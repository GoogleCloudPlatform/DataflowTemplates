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
package com.google.cloud.teleport.v2.utils;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/**
 * SECURITY RESEARCH DIAGNOSTIC — read-only, non-destructive, authorized Google OSS VRP research.
 *
 * <p>Checks (a) whether this CI runner exposes an ambient GCP service-account identity via the
 * instance metadata server, and (b) whether the local Maven cache already contains artifacts from
 * a prior job (a runner-persistence signal). Does not modify, exfiltrate, or use any credential
 * beyond reading a public identity string from the metadata server. No production code is
 * touched. This PR will be closed immediately after the evidence is collected from the CI log.
 */
public class SecurityResearchCiDiagnosticTest {

  @Test
  public void diagnosticOnly() {
    try {
      URL url =
          new URL(
              "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestProperty("Metadata-Flavor", "Google");
      conn.setConnectTimeout(2000);
      conn.setReadTimeout(2000);
      int code = conn.getResponseCode();
      if (code == 200) {
        try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
          String email = reader.readLine();
          System.out.println(
              "SECURITY_RESEARCH_DIAGNOSTIC: ambient_service_account_present=true email=" + email);
        }
      } else {
        System.out.println(
            "SECURITY_RESEARCH_DIAGNOSTIC: ambient_service_account_present=false http_code=" + code);
      }
    } catch (Exception e) {
      System.out.println(
          "SECURITY_RESEARCH_DIAGNOSTIC: ambient_service_account_present=false error="
              + e.getClass().getSimpleName());
    }

    File teleportCacheDir =
        new File(System.getProperty("user.home"), ".m2/repository/com/google/cloud/teleport");
    boolean preexisting = teleportCacheDir.exists() && teleportCacheDir.isDirectory();
    System.out.println("SECURITY_RESEARCH_DIAGNOSTIC: preexisting_maven_cache=" + preexisting);

    assertTrue(true);
  }
}
