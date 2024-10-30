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
import java.util.*;
import java.util.concurrent.*;

import org.apache.commons.lang3.tuple.Pair;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED_DEFAULT;
import static org.apache.hadoop.hdds.scm.net.NetConstants.NODE_COST_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCH_MAX_BLOCKS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCH_MAX_BLOCKS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCH_MIN_BLOCKS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCH_MIN_BLOCKS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCHED_BLOCKS_VALIDATION_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCHED_BLOCKS_VALIDATION_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;

public class OMBlockPrefetchClient {
    private static final Logger LOG = LoggerFactory.getLogger(OMBlockPrefetchClient.class);
    private static ScmBlockLocationProtocol scmBlockLocationProtocol = null;
    private static StorageContainerLocationProtocol scmContainerClient = null;
    private static final LinkedList<AllocatedBlock> blockQueueRatisOne = new LinkedList<>();
    private static final LinkedList<AllocatedBlock> blockQueueRatisThree = new LinkedList<>();
    private static final LinkedList<AllocatedBlock> blockQueueRS_3_2 = new LinkedList<>();
    private static final LinkedList<AllocatedBlock> blockQueueRS_6_3 = new LinkedList<>();
    private static final LinkedList<AllocatedBlock> blockQueueXOR_10_4 = new LinkedList<>();
    private static ConcurrentLinkedQueue<Pair<String, ExcludeList>> excludeListQueue = new ConcurrentLinkedQueue<>();
    private ScheduledExecutorService executorService;
    private static int maxBlocks;
    private static int minBlocks;
    private static boolean useHostname;
    private static DNSToSwitchMapping dnsToSwitchMapping;
    private boolean grpcBlockTokenEnabled;
    private int preallocateBlocksMax;
    private static long scmBlockSize;
    private long checkInterval;
    private static String serviceID;
    private static final ReplicationConfig RATIS_THREE =
            ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
                    HddsProtos.ReplicationFactor.THREE);
    private static final ReplicationConfig RATIS_ONE =
            ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
                    HddsProtos.ReplicationFactor.ONE);
    private static final ReplicationConfig RS_3_2_1024 = ReplicationConfig.fromProto(HddsProtos.ReplicationType.EC, null, OMBlockPrefetchClient.toProto(3, 2, ECReplicationConfig.EcCodec.RS, 1024));
    private static final ReplicationConfig RS_6_3_1024 = ReplicationConfig.fromProto(HddsProtos.ReplicationType.EC, null, OMBlockPrefetchClient.toProto(6, 3, ECReplicationConfig.EcCodec.RS, 1024));
    private static final ReplicationConfig XOR_10_4_4096 = ReplicationConfig.fromProto(HddsProtos.ReplicationType.EC, null, OMBlockPrefetchClient.toProto(10, 4, ECReplicationConfig.EcCodec.XOR, 4096));

    public static HddsProtos.ECReplicationConfig toProto(int data, int parity, ECReplicationConfig.EcCodec codec, int ecChunkSize) {
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
    }

    public void start(ConfigurationSource conf) throws IOException, InterruptedException, TimeoutException {
        maxBlocks = conf.getInt(OZONE_OM_PREFETCH_MAX_BLOCKS, OZONE_OM_PREFETCH_MAX_BLOCKS_DEFAULT);
        minBlocks = conf.getInt(OZONE_OM_PREFETCH_MIN_BLOCKS, OZONE_OM_PREFETCH_MIN_BLOCKS_DEFAULT);
        this.grpcBlockTokenEnabled = conf.getBoolean(HDDS_BLOCK_TOKEN_ENABLED, HDDS_BLOCK_TOKEN_ENABLED_DEFAULT);
        this.preallocateBlocksMax = conf.getInt(OZONE_KEY_PREALLOCATION_BLOCKS_MAX, OZONE_KEY_PREALLOCATION_BLOCKS_MAX_DEFAULT);
        scmBlockSize = (long) conf.getStorageSize(OZONE_SCM_BLOCK_SIZE,
                OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);
        useHostname = conf.getBoolean(
            DFSConfigKeysLegacy.DFS_DATANODE_USE_DN_HOSTNAME,
            DFSConfigKeysLegacy.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);

        // waitTobeOutOfSafeMode();

//        prefetchBlocks(maxBlocks, RATIS_THREE, blockQueueRatisThree);
//        prefetchBlocks(maxBlocks, RATIS_ONE, blockQueueRatisOne);
//        prefetchBlocks(maxBlocks, RS_3_2_1024, blockQueueRS_3_2);
//        prefetchBlocks(maxBlocks, RS_6_3_1024, blockQueueRS_6_3);
//        prefetchBlocks(maxBlocks, XOR_10_4_4096, blockQueueXOR_10_4);

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

    /*
    public static class CustomWaitUtils {

        public static void waitForCondition(CheckCondition condition, long checkIntervalMs, long timeoutMs)
            throws TimeoutException, InterruptedException, IOException {
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < timeoutMs) {
                if (condition.evaluate()) {
                    return;
                }
                Thread.sleep(checkIntervalMs);
            }
            throw new TimeoutException("Condition not met within timeout period");
        }

        @FunctionalInterface
        public interface CheckCondition {
            boolean evaluate() throws IOException;
        }
    }

    public void waitTobeOutOfSafeMode() throws TimeoutException, InterruptedException, IOException {
        CustomWaitUtils.waitForCondition(() -> {
            if (!scmContainerClient.inSafeMode()) {
                return true;
            }
            LOG.info("Waiting for cluster to be ready. No datanodes found");
            return false;
        }, 100, 1000 * 45);
    }*/

    public static void waitTobeOutOfSafeMode() throws InterruptedException, IOException {
        while (true) {
            if (!scmContainerClient.inSafeMode()) {
                return;
            }
            LOG.info("Waiting for cluster to be ready.");
            Thread.sleep(5000);
        }
    }

    // Stop the background tasks
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
        excludeListQueue.removeIf(pair -> pair.getLeft().equals(clientMachine));
        excludeListQueue.add(Pair.of(clientMachine, excludeList));
    }

    private static synchronized void prefetchBlocks(int numBlocks, ReplicationConfig replicationConfig,
                                                    LinkedList<AllocatedBlock> blockQueue) throws IOException {
        LOG.info("Prefetching {} AllocatedBlocks from SCM", numBlocks);
        List<AllocatedBlock> replicatedBlocks = allocateBlocksWithReplicationForce(replicationConfig, numBlocks);
        blockQueue.addAll(replicatedBlocks);
    }

    private static List<AllocatedBlock> allocateBlocksWithReplicationForce(
        ReplicationConfig replicationConfig, int blockCount) throws IOException {
        return scmBlockLocationProtocol.allocateBlock(
                scmBlockSize, blockCount, replicationConfig, serviceID, new ExcludeList(), "", true);
    }

    private static List<AllocatedBlock> allocateBlocksWithReplication(
        ReplicationConfig replicationConfig, int blockCount) throws IOException {
        return scmBlockLocationProtocol.allocateBlock(
                scmBlockSize, blockCount, replicationConfig, serviceID, new ExcludeList(), "");
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
                System.out.println("Refresh duration: " + refreshDuration.toMillis());
                validateAndRefillBlocks(conf, excludeListQueue);
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

    private synchronized void validateAndRefillBlocks(ConfigurationSource conf,
                                                      ConcurrentLinkedQueue<Pair<String, ExcludeList>> excludeListQueue)
        throws IOException, InterruptedException {
        waitTobeOutOfSafeMode();
        LOG.info("Validating cached allocated blocks...");
        validateAndRefillBlocksUtil(blockQueueRatisOne, RATIS_ONE, excludeListQueue);
        validateAndRefillBlocksUtil(blockQueueRatisThree, RATIS_THREE, excludeListQueue);
//        validateAndRefillBlocksUtil(blockQueueRS_3_2, RS_3_2_1024, excludeListQueue);
//        validateAndRefillBlocksUtil(blockQueueRS_6_3, RS_6_3_1024, excludeListQueue);
//        validateAndRefillBlocksUtil(blockQueueXOR_10_4, XOR_10_4_4096, excludeListQueue);
    }

    private static void validateAndRefillBlocksUtil(LinkedList<AllocatedBlock> blockQueue,
                                                    ReplicationConfig replicationConfig,
                                                    ConcurrentLinkedQueue<Pair<String, ExcludeList>> excludeListQueue)
        throws IOException, InterruptedException {

        blockQueue.removeIf(block -> {
            boolean isValid = isBlockValid(block, excludeListQueue);
            if (!isValid) {
                LOG.info("Block {} is no longer valid and will be replaced", block.getBlockID());
            }
            return !isValid;
        });

        if (blockQueue.size() < minBlocks) {
            int blocksToPrefetch = maxBlocks - blockQueue.size();
            LOG.info("Block queue size below threshold. Fetching {} more blocks.", blocksToPrefetch);
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
                                                             String clientMachine, NetworkTopology clusterMap, ExcludeList excludeList) {
        queueExcludeList(clientMachine, excludeList);
        LinkedList<AllocatedBlock> blockQueue = getReplicationConfigQueue(replicationConfig);
        List<AllocatedBlock> allocatedBlocks = new ArrayList<>();
        for (int i = 0; i < numBlocks && !blockQueue.isEmpty(); i++) {
            AllocatedBlock block = blockQueue.removeFirst();
            List<DatanodeDetails> sorted = sortDatanodes(block.getPipeline().getNodes(), clientMachine, clusterMap);
            if (!Objects.equals(sorted, block.getPipeline().getNodesInOrder())) {
                block = block.toBuilder()
                    .setPipeline(block.getPipeline().copyWithNodesInOrder(sorted))
                    .build();
            }
            allocatedBlocks.add(block);
        }
        LOG.info("Returning {} blocks for replication config {}", allocatedBlocks.size(), replicationConfig);
        return allocatedBlocks;
    }

    public static boolean queueSufficient(int numBlocks, ReplicationConfig replicationConfig)
        throws IOException, InterruptedException {
        if (numBlocks > getReplicationConfigQueue(replicationConfig).size()) {
            validateAndRefillBlocksUtil(getReplicationConfigQueue(replicationConfig), replicationConfig, excludeListQueue);
            return false;
        }
        return true;
    }

    private static LinkedList<AllocatedBlock> getReplicationConfigQueue(ReplicationConfig replicationConfig) {

        LinkedList<AllocatedBlock> blockQueue = null;

        if (replicationConfig.equals(RATIS_THREE)) {
            blockQueue = blockQueueRatisThree;
        } else if (replicationConfig.equals(RATIS_ONE)) {
            blockQueue = blockQueueRatisOne;
        } else if (replicationConfig.equals(RS_3_2_1024)) {
            blockQueue = blockQueueRS_3_2;
        } else if (replicationConfig.equals(RS_6_3_1024)) {
            blockQueue = blockQueueRS_6_3;
        } else if (replicationConfig.equals(XOR_10_4_4096)) {
            blockQueue = blockQueueXOR_10_4;
        }
        return blockQueue;
    }

    public static List<DatanodeDetails> sortDatanodes(List<DatanodeDetails> nodes,
                                                      String clientMachine, NetworkTopology clusterMap) {
        final Node client = getClientNode(clientMachine, nodes, clusterMap);
        return clusterMap.sortByDistanceCost(client, nodes, nodes.size());
    }

    private static Node getClientNode(String clientMachine,
                                      List<DatanodeDetails> nodes, NetworkTopology clusterMap) {
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
            LOG.info("Could not resolve client {}: {}",
                clientMachine, e.getMessage());
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