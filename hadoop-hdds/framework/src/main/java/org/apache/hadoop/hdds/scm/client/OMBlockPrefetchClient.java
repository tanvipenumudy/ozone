package org.apache.hadoop.hdds.scm.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
    private final LinkedList<AllocatedBlock> blockQueue = new LinkedList<>();
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
        prefetchBlocks(maxBlocks);
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

    private synchronized void prefetchBlocks(int numBlocks) throws IOException {
        LOG.info("Prefetching {} AllocatedBlocks from SCM", numBlocks);

        List<AllocatedBlock> ratisThreeReplicatedBlocks = allocateBlocksWithReplication(RATIS_THREE, numBlocks / 2);
        List<AllocatedBlock> ratisOneReplicatedBlocks = allocateBlocksWithReplication(RATIS_ONE, numBlocks / 2);

        blockQueue.addAll(ratisThreeReplicatedBlocks);
        blockQueue.addAll(ratisOneReplicatedBlocks);
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
            prefetchBlocks(blocksToPrefetch);
        }
    }

    private boolean isBlockValid(AllocatedBlock block) {


    }

    public synchronized AllocatedBlock getBlock() {
        return blockQueue.pollFirst();
    }
}