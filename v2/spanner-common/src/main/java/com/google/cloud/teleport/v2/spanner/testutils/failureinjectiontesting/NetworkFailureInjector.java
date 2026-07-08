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
package com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.compute.v1.Denied;
import com.google.cloud.compute.v1.Firewall;
import com.google.cloud.compute.v1.FirewallsClient;
import com.google.cloud.compute.v1.Operation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.beam.it.common.ResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkFailureInjector implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(NetworkFailureInjector.class);
  private final String projectId;
  private final String networkName;
  private final FirewallsClient firewallsClient;
  private final List<String> firewallRuleNames = new ArrayList<>();

  private NetworkFailureInjector(Builder builder) {
    this.projectId = builder.projectId;
    this.networkName = builder.networkName;
    try {
      this.firewallsClient = FirewallsClient.create();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create FirewallsClient", e);
    }
  }

  public static Builder builder(String projectId, String networkName) {
    return new Builder(projectId, networkName);
  }

  /**
   * Blocks network connectivity for a host with a tag in a VPC network for a specific port by
   * adding a firewall rule.
   *
   * @param targetTag The tag of the host to block network connectivity for.
   * @param port The port to block.
   */
  public void blockNetwork(String targetTag, int port)
      throws ExecutionException, InterruptedException {

    String ruleId = "network-failure-" + UUID.randomUUID();
    String ingressFirewallRuleName = ruleId + "-ingress";
    String egressFirewallRuleName = ruleId + "-egress";
    String networkUrl = String.format("projects/%s/global/networks/%s", projectId, networkName);

    Denied deniedRule =
        Denied.newBuilder().setIPProtocol("tcp").addPorts(String.valueOf(port)).build();

    Firewall ingressFirewallRule =
        Firewall.newBuilder()
            .setName(ingressFirewallRuleName)
            .setNetwork(networkUrl)
            .setDirection("INGRESS")
            .setPriority(100) // High priority
            .addAllDenied(Collections.singletonList(deniedRule))
            .addAllTargetTags(Collections.singletonList(targetTag))
            .addAllSourceRanges(Collections.singletonList("0.0.0.0/0"))
            .build();

    Firewall egressFirewallRule =
        Firewall.newBuilder()
            .setName(egressFirewallRuleName)
            .setNetwork(networkUrl)
            .setDirection("EGRESS")
            .setPriority(100) // High priority
            .addAllDenied(Collections.singletonList(deniedRule))
            .addAllTargetTags(Collections.singletonList(targetTag))
            .addAllDestinationRanges(Collections.singletonList("0.0.0.0/0"))
            .build();

    LOG.info(
        "Injecting network failure by creating firewall rules '{}' and '{}' to block port {} for tag '{}'...",
        ingressFirewallRuleName,
        egressFirewallRuleName,
        port,
        targetTag);

    OperationFuture<Operation, Operation> insertIngressOperationFuture =
        firewallsClient.insertAsync(projectId, ingressFirewallRule);
    OperationFuture<Operation, Operation> insertEgressOperationFuture =
        firewallsClient.insertAsync(projectId, egressFirewallRule);

    Operation insertIngressResponse = insertIngressOperationFuture.get();
    Operation insertEgressResponse = insertEgressOperationFuture.get();

    if (insertIngressResponse.hasError()) {
      LOG.error(
          "Error creating firewall rule '{}': {}",
          ingressFirewallRuleName,
          insertIngressResponse.getError());
      // Clean up egress rule if ingress failed
      firewallsClient.deleteAsync(projectId, egressFirewallRuleName).get();
      throw new RuntimeException(
          "Failed to create ingress firewall rule: " + insertIngressResponse.getError().toString());
    }
    if (insertEgressResponse.hasError()) {
      LOG.error(
          "Error creating firewall rule '{}': {}",
          egressFirewallRuleName,
          insertEgressResponse.getError());
      // Clean up ingress rule if egress failed
      firewallsClient.deleteAsync(projectId, ingressFirewallRuleName).get();
      throw new RuntimeException(
          "Failed to create egress firewall rule: " + insertEgressResponse.getError().toString());
    }

    firewallRuleNames.add(ingressFirewallRuleName);
    firewallRuleNames.add(egressFirewallRuleName);

    LOG.info(
        "-> Firewall rules '{}' and '{}' created successfully.",
        ingressFirewallRuleName,
        egressFirewallRuleName);
  }

  @Override
  public void cleanupAll() {
    LOG.info("Cleaning up network failure resources.");
    List<OperationFuture<Operation, Operation>> deleteFutures = new ArrayList<>();
    for (String ruleName : firewallRuleNames) {
      LOG.info("Removing firewall rule '{}' to restore connectivity...", ruleName);
      deleteFutures.add(firewallsClient.deleteAsync(projectId, ruleName));
    }

    for (int i = 0; i < deleteFutures.size(); i++) {
      String ruleName = firewallRuleNames.get(i);
      try {
        Operation response = deleteFutures.get(i).get();
        if (response.hasError()) {
          LOG.error("Error deleting firewall rule '{}': {}", ruleName, response.getError());
        } else {
          LOG.info("-> Firewall rule '{}' deleted successfully.", ruleName);
        }
      } catch (InterruptedException | ExecutionException e) {
        if (e.getCause() instanceof NotFoundException) {
          LOG.info("Firewall rule '{}' was already deleted.", ruleName);
        } else {
          LOG.warn(
              "Failed to delete firewall rule {}. This can lead to resource leaking.", ruleName, e);
        }
      }
    }
    firewallRuleNames.clear();
    firewallsClient.close();
  }

  /** Builder for {@link NetworkFailureInjector}. */
  public static final class Builder {
    private final String projectId;
    private final String networkName;

    private Builder(String projectId, String networkName) {
      this.projectId = projectId;
      this.networkName = networkName;
    }

    public NetworkFailureInjector build() {
      return new NetworkFailureInjector(this);
    }
  }
}
