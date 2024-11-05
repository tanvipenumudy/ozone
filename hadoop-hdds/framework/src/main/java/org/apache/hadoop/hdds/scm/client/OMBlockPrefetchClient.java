/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.InnerNode;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.net.NodeImpl;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.TableMapping;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;


import org.apache.commons.lang3.tuple.Pair;

import static org.apache.hadoop.hdds.scm.net.NetConstants.NODE_COST_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCH_MAX_BLOCKS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCH_MAX_BLOCKS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCH_MIN_BLOCKS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCH_MIN_BLOCKS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCHED_BLOCKS_VALIDATION_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCHED_BLOCKS_VALIDATION_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;

/**
 * OMBlockPrefetchClient prefetches blocks from SCM for quicker write operations.
 * It maintains a queue of allocated blocks categorized by replication configurations and validates them periodically.
 * This client refills the queue in background by allocating additional blocks as needed, ensuring availability during
 * high-throughput scenarios. It also manages an exclude list to avoid nodes that should
 * not be used during block allocation.
 */
public class OMBlockPrefetchClient {
  private static final Logger LOG = LoggerFactory.getLogger(OMBlockPrefetchClient.class);
  private static ScmBlockLocationProtocol scmBlockLocationProtocol = null;
  private static StorageContainerLocationProtocol scmContainerClient = null;
  private static final Map<ReplicationConfig, LinkedList<AllocatedBlock>> blockQueueMap = new HashMap<>();
  private static ConcurrentLinkedQueue<Pair<String, ExcludeList>> excludeListQueue = new ConcurrentLinkedQueue<>();
  private ScheduledExecutorService executorService;
  private static int maxBlocks;
  private static int minBlocks;
  private static boolean useHostname;
  private static DNSToSwitchMapping dnsToSwitchMapping;
  private static long scmBlockSize;
  private static long maxContainerSize;
  private static String serviceID;
  private static final ReplicationConfig RATIS_THREE =
      ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE);
  private static final ReplicationConfig RATIS_ONE =
      ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.ONE);
  private static final ReplicationConfig RS_3_2_1024 =
      ReplicationConfig.fromProto(HddsProtos.ReplicationType.EC, null,
          OMBlockPrefetchClient.toProto(3, 2, ECReplicationConfig.EcCodec.RS, 1024));
  private static final ReplicationConfig RS_6_3_1024 =
      ReplicationConfig.fromProto(HddsProtos.ReplicationType.EC, null,
          OMBlockPrefetchClient.toProto(6, 3, ECReplicationConfig.EcCodec.RS, 1024));
  private static final ReplicationConfig XOR_10_4_4096 =
      ReplicationConfig.fromProto(HddsProtos.ReplicationType.EC, null,
          OMBlockPrefetchClient.toProto(10, 4, ECReplicationConfig.EcCodec.XOR, 4096));

  public static HddsProtos.ECReplicationConfig toProto(int data, int parity, ECReplicationConfig.EcCodec codec,
                                                       int ecChunkSize) {
    return HddsProtos.ECReplicationConfig.newBuilder()
        .setData(data)
        .setParity(parity)
        .setCodec(codec.toString())
        .setEcChunkSize(ecChunkSize)
        .build();
  }

  public OMBlockPrefetchClient(ScmBlockLocationProtocol scmBlockClient,
                               StorageContainerLocationProtocol scmContainerClient, String serviceID) {
    this.scmBlockLocationProtocol = scmBlockClient;
    this.serviceID = serviceID;
    this.scmContainerClient = scmContainerClient;

    blockQueueMap.put(RATIS_THREE, new LinkedList<>());
    blockQueueMap.put(RATIS_ONE, new LinkedList<>());
    blockQueueMap.put(RS_3_2_1024, new LinkedList<>());
    blockQueueMap.put(RS_6_3_1024, new LinkedList<>());
    blockQueueMap.put(XOR_10_4_4096, new LinkedList<>());
  }

  public void start(ConfigurationSource conf) throws IOException, InterruptedException, TimeoutException {
    maxBlocks = conf.getInt(OZONE_OM_PREFETCH_MAX_BLOCKS, OZONE_OM_PREFETCH_MAX_BLOCKS_DEFAULT);
    minBlocks = conf.getInt(OZONE_OM_PREFETCH_MIN_BLOCKS, OZONE_OM_PREFETCH_MIN_BLOCKS_DEFAULT);
    scmBlockSize =
        (long) conf.getStorageSize(OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);
    maxContainerSize = (long) conf.getStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    useHostname = conf.getBoolean(DFSConfigKeysLegacy.DFS_DATANODE_USE_DN_HOSTNAME,
        DFSConfigKeysLegacy.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);

    Class<? extends DNSToSwitchMapping> dnsToSwitchMappingClass =
        conf.getClass(DFSConfigKeysLegacy.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            TableMapping.class, DNSToSwitchMapping.class);
    DNSToSwitchMapping newInstance = ReflectionUtils.newInstance(
        dnsToSwitchMappingClass, OzoneConfiguration.of(conf));
    dnsToSwitchMapping =
        ((newInstance instanceof CachedDNSToSwitchMapping) ? newInstance
            : new CachedDNSToSwitchMapping(newInstance));

    scheduleBlockValidationAndFetch(conf, Instant.now());
  }

  public static void waitTobeOutOfSafeMode() throws InterruptedException, IOException {
    while (true) {
      if (!scmContainerClient.inSafeMode()) {
        return;
      }
      LOG.info("Waiting for cluster to be ready.");
      Thread.sleep(5000);
    }
  }

  public void stop() {
    if (executorService != null) {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOG.error("Interrupted while shutting down executor service.", e);
        Thread.currentThread().interrupt();
      }
    }
  }

  private static void queueExcludeList(String clientMachine, ExcludeList excludeList) {
    if (excludeList != null && !excludeList.isEmpty()) {
      excludeListQueue.removeIf(pair -> pair.getLeft().equals(clientMachine));
      excludeListQueue.add(Pair.of(clientMachine, excludeList));
    }
  }

  private static synchronized void prefetchBlocks(int numBlocks, ReplicationConfig replicationConfig,
                                                  LinkedList<AllocatedBlock> blockQueue) throws IOException {
    LOG.info("Prefetching {} blocks from SCM", numBlocks);
    long blocksPerContainer = maxContainerSize / scmBlockSize;
    int containersNeeded = (int) Math.ceil((double) numBlocks / blocksPerContainer);
    List<AllocatedBlock> blocks;
    try {
      if (numBlocks <= containersNeeded) {
        blocks = allocateBlocksWithReplication(replicationConfig, numBlocks, true);
      } else {
        blocks = allocateBlocksWithReplication(replicationConfig, containersNeeded, true);
        blocks.addAll(allocateBlocksWithReplication(replicationConfig, numBlocks - containersNeeded, false));
      }
      blockQueue.addAll(blocks);
    } catch (SCMException ex) {
      LOG.warn("SCMException occurred: {}. Attempting to fetch a different replication configuration.",
          ex.getMessage());
      return;
    }
  }

  private static List<AllocatedBlock> allocateBlocksWithReplication(
      ReplicationConfig replicationConfig, int blockCount, boolean forceContainerCreate) throws IOException {
    return scmBlockLocationProtocol.allocateBlock(scmBlockSize, blockCount, replicationConfig, serviceID,
        new ExcludeList(), null, forceContainerCreate);
  }

  private void scheduleBlockValidationAndFetch(ConfigurationSource conf,
                                               Instant initialInvocation) {
    Duration refreshDuration = parseRefreshDuration(conf);
    Instant nextRefresh = initialInvocation.plus(refreshDuration);
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("BlockPrefetchAndValidationThread")
        .setDaemon(true)
        .build();
    executorService = Executors.newScheduledThreadPool(1, threadFactory);
    Duration initialDelay = Duration.between(Instant.now(), nextRefresh);

    executorService.scheduleAtFixedRate(() -> {
      try {
        excludeListQueue = new ConcurrentLinkedQueue<>();
        validateAndRefillBlocks(excludeListQueue);
      } catch (IOException | InterruptedException ex) {
        LOG.error("Error in block validation or refill", ex);
      }
    }, initialDelay.toMillis(), refreshDuration.toMillis(), TimeUnit.MILLISECONDS);
  }

  public static Duration parseRefreshDuration(ConfigurationSource conf) {
    long refreshDurationInMs = conf.getTimeDuration(
        OZONE_OM_PREFETCHED_BLOCKS_VALIDATION_INTERVAL,
        OZONE_OM_PREFETCHED_BLOCKS_VALIDATION_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
    return Duration.ofMillis(refreshDurationInMs);
  }

  private synchronized void validateAndRefillBlocks(ConcurrentLinkedQueue<Pair<String, ExcludeList>> excludeListQueue)
      throws IOException, InterruptedException {
    waitTobeOutOfSafeMode();

    LOG.debug("Validating cached AllocatedBlocks...");
    for (Map.Entry<ReplicationConfig, LinkedList<AllocatedBlock>> entry : blockQueueMap.entrySet()) {
      ReplicationConfig replicationConfig = entry.getKey();
      LinkedList<AllocatedBlock> blockQueue = entry.getValue();
      validateAndRefillBlocksUtil(blockQueue, replicationConfig, excludeListQueue);
    }
  }

  private static void validateAndRefillBlocksUtil(LinkedList<AllocatedBlock> blockQueue,
                                                  ReplicationConfig replicationConfig,
                                                  ConcurrentLinkedQueue<Pair<String, ExcludeList>> excludeListQueue)
      throws IOException {
    blockQueue.removeIf(block -> {
      boolean isValid = isBlockValid(block, excludeListQueue);
      if (!isValid) {
        LOG.debug("Block {} is no longer valid and will be replaced", block.getBlockID());
      }
      return !isValid;
    });

    if (blockQueue.size() < minBlocks) {
      int blocksToPrefetch = maxBlocks - blockQueue.size();
      LOG.info("Block queue size below threshold");
      prefetchBlocks(blocksToPrefetch, replicationConfig, blockQueue);
    }
  }

  private static boolean isBlockValid(AllocatedBlock block,
                                      ConcurrentLinkedQueue<Pair<String, ExcludeList>> excludeListQueue) {
    ContainerBlockID blockID = block.getBlockID();
    Pipeline pipeline = block.getPipeline();

    for (Pair<String, ExcludeList> entry : excludeListQueue) {
      ExcludeList excludeList = entry.getRight();
      for (DatanodeDetails datanode : pipeline.getNodes()) {
        if (excludeList.getDatanodes().contains(datanode)) {
          return false;
        }
      }
      if (excludeList.getContainerIds().contains(blockID.getContainerID())) {
        return false;
      }
      if (excludeList.getPipelineIds().contains(pipeline.getId())) {
        return false;
      }
    }
    return true;
  }

  public synchronized static List<AllocatedBlock> getBlock(int numBlocks, ReplicationConfig replicationConfig,
                                                           String clientMachine, NetworkTopology clusterMap,
                                                           ExcludeList excludeList) {
    queueExcludeList(clientMachine, excludeList);
    LinkedList<AllocatedBlock> blockQueue = blockQueueMap.get(replicationConfig);
    List<AllocatedBlock> allocatedBlocks = new ArrayList<>();
    for (int i = 0; i < numBlocks && !blockQueue.isEmpty(); i++) {
      AllocatedBlock block = blockQueue.removeFirst();
      List<DatanodeDetails> nodes = block.getPipeline().getNodes();
      final Node client = getClientNode(clientMachine, nodes, clusterMap);
      if (client != null) {
        List<DatanodeDetails> sorted = clusterMap.sortByDistanceCost(client, nodes, nodes.size());
        if (!Objects.equals(sorted, block.getPipeline().getNodesInOrder())) {
          block = block.toBuilder()
              .setPipeline(block.getPipeline().copyWithNodesInOrder(sorted))
              .build();
        }
      }
      allocatedBlocks.add(block);
    }
    LOG.info("Returning {} blocks for replication config {}", allocatedBlocks.size(), replicationConfig);
    return allocatedBlocks;
  }

  public static boolean queueSufficient(int numBlocks, ReplicationConfig replicationConfig)
      throws IOException, InterruptedException {
    LinkedList<AllocatedBlock> blockQueue = blockQueueMap.get(replicationConfig);
    if (numBlocks > blockQueue.size()) {
      validateAndRefillBlocksUtil(blockQueue, replicationConfig, excludeListQueue);
      return false;
    }
    return true;
  }

  private static Node getClientNode(String clientMachine, List<DatanodeDetails> nodes, NetworkTopology clusterMap) {
    List<DatanodeDetails> matchingNodes = new ArrayList<>();
    for (DatanodeDetails node : nodes) {
      if ((useHostname ? node.getHostName() : node.getIpAddress()).equals(
          clientMachine)) {
        matchingNodes.add(node);
      }
    }
    return !matchingNodes.isEmpty() ? matchingNodes.get(0) :
        getOtherNode(clientMachine, clusterMap);
  }

  private static Node getOtherNode(String clientMachine, NetworkTopology clusterMap) {
    try {
      String clientLocation = resolveNodeLocation(clientMachine);
      if (clientLocation != null) {
        Node rack = clusterMap.getNode(clientLocation);
        if (rack instanceof InnerNode) {
          return new NodeImpl(clientMachine, clientLocation,
              (InnerNode) rack, rack.getLevel() + 1,
              NODE_COST_DEFAULT);
        }
      }
    } catch (Exception e) {
      LOG.info("Could not resolve client {}: {}", clientMachine, e.getMessage());
    }
    return null;
  }

  private static String resolveNodeLocation(String hostname) {
    List<String> hosts = Collections.singletonList(hostname);
    List<String> resolvedHosts = dnsToSwitchMapping.resolve(hosts);
    if (resolvedHosts != null && !resolvedHosts.isEmpty()) {
      String location = resolvedHosts.get(0);
      LOG.debug("Node {} resolved to location {}", hostname, location);
      return location;
    } else {
      LOG.debug("Node resolution did not yield any result for {}", hostname);
      return null;
    }
  }
}
