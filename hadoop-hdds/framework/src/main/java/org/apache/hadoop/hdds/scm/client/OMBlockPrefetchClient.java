package org.apache.hadoop.hdds.scm.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED_DEFAULT;
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
    private final ScmBlockLocationProtocol scmBlockLocationProtocol;
    private final LinkedList<AllocatedBlock> blockQueueRatisOne = new LinkedList<>();
    private final LinkedList<AllocatedBlock> blockQueueRatisThree = new LinkedList<>();
    private final LinkedList<AllocatedBlock> blockQueueRS_3_2 = new LinkedList<>();
    private final LinkedList<AllocatedBlock> blockQueueRS_6_3 = new LinkedList<>();
    private final LinkedList<AllocatedBlock> blockQueueXOR_10_4 = new LinkedList<>();

    private ScheduledExecutorService executorService;
    private int maxBlocks, minBlocks;
    private boolean grpcBlockTokenEnabled;
    private int preallocateBlocksMax;
    private long scmBlockSize;
    private long checkInterval;
    private String serviceID;
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

    public OMBlockPrefetchClient(ScmBlockLocationProtocol scmBlockLocationProtocol, String serviceID) {
        this.scmBlockLocationProtocol = scmBlockLocationProtocol;
        this.serviceID = serviceID;
    }

    public void start(ConfigurationSource conf) throws IOException {
        this.maxBlocks = conf.getInt(OZONE_OM_PREFETCH_MAX_BLOCKS, OZONE_OM_PREFETCH_MAX_BLOCKS_DEFAULT);
        this.minBlocks = conf.getInt(OZONE_OM_PREFETCH_MIN_BLOCKS, OZONE_OM_PREFETCH_MIN_BLOCKS_DEFAULT);
        this.grpcBlockTokenEnabled = conf.getBoolean(HDDS_BLOCK_TOKEN_ENABLED, HDDS_BLOCK_TOKEN_ENABLED_DEFAULT);
        this.preallocateBlocksMax = conf.getInt(OZONE_KEY_PREALLOCATION_BLOCKS_MAX, OZONE_KEY_PREALLOCATION_BLOCKS_MAX_DEFAULT);
        this.scmBlockSize = (long) conf.getStorageSize(OZONE_SCM_BLOCK_SIZE,
                OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);
        prefetchBlocks(maxBlocks, RATIS_THREE, blockQueueRatisThree);
        prefetchBlocks(maxBlocks, RATIS_ONE, blockQueueRatisOne);
        prefetchBlocks(maxBlocks, RS_3_2_1024, blockQueueRS_3_2);
        prefetchBlocks(maxBlocks, RS_6_3_1024, blockQueueRS_6_3);
        prefetchBlocks(maxBlocks, XOR_10_4_4096, blockQueueXOR_10_4);
        scheduleBlockValidationAndFetch(conf, Instant.now());
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

    private synchronized void prefetchBlocks(int numBlocks, ReplicationConfig replicationConfig,
                                             LinkedList<AllocatedBlock> blockQueue) throws IOException {
        LOG.info("Prefetching {} AllocatedBlocks from SCM", numBlocks);
        List<AllocatedBlock> replicatedBlocks = allocateBlocksWithReplication(replicationConfig, numBlocks);
        blockQueue.addAll(replicatedBlocks);
    }

    private List<AllocatedBlock> allocateBlocksWithReplicationForce(
            ReplicationConfig replicationConfig, int blockCount) throws IOException {
        return scmBlockLocationProtocol.allocateBlock(
                scmBlockSize, blockCount, replicationConfig, serviceID, new ExcludeList(), "", true);
    }

    private List<AllocatedBlock> allocateBlocksWithReplication(
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
                validateAndRefillBlocks(conf);
            } catch (IOException ex) {
                LOG.error("Error in block validation or refill", ex);
            }
        }, initialDelay.toMillis(), refreshDuration.toMillis(), TimeUnit.MILLISECONDS);
    }

    public static Duration parseRefreshDuration(ConfigurationSource conf) {
        long refreshDurationInMs = conf.getTimeDuration(
                OZONE_OM_PREFETCHED_BLOCKS_VALIDATION_INTERVAL,
                OZONE_OM_PREFETCHED_BLOCKS_VALIDATION_INTERVAL_DEFAULT, TimeUnit.SECONDS);
        return Duration.ofMillis(refreshDurationInMs);
    }

    private synchronized void validateAndRefillBlocks(ConfigurationSource conf) throws IOException {
        LOG.info("Validating cached allocated blocks...");
        validateAndRefillBlocksUtil(blockQueueRatisOne, RATIS_ONE, conf);
        validateAndRefillBlocksUtil(blockQueueRatisThree, RATIS_THREE, conf);
        validateAndRefillBlocksUtil(blockQueueRS_3_2, RS_3_2_1024, conf);
        validateAndRefillBlocksUtil(blockQueueRS_6_3, RS_6_3_1024, conf);
        validateAndRefillBlocksUtil(blockQueueXOR_10_4, XOR_10_4_4096, conf);
    }

    private void validateAndRefillBlocksUtil(LinkedList<AllocatedBlock> blockQueue, ReplicationConfig replicationConfig,
                                              ConfigurationSource conf) throws IOException  {
        blockQueue.removeIf(block -> {
            boolean isValid = isBlockValid(block);
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

    private boolean isBlockValid(AllocatedBlock block) {
        return true;
    }

    public synchronized AllocatedBlock getBlock() {
        return blockQueue.pollFirst();
    }
}