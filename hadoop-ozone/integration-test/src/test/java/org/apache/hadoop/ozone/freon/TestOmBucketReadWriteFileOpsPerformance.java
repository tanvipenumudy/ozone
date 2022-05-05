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
package org.apache.hadoop.ozone.freon;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.lock.OMLockMetrics;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Test for OmBucketReadWriteFileOps.
 */
public class TestOmBucketReadWriteFileOpsPerformance {

  private String path;
  private OzoneConfiguration conf = null;
  private MiniOzoneCluster cluster = null;
  private ObjectStore store = null;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestOmBucketReadWriteFileOpsPerformance.class);

  @Before
  public void setup() {
    path = GenericTestUtils
        .getTempPath(TestOmBucketReadWriteFileOpsPerformance.class.getSimpleName());
    GenericTestUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    File baseDir = new File(path);
    baseDir.mkdirs();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  private void shutdown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      FileUtils.deleteDirectory(new File(path));
    }
  }

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  private void startCluster() throws Exception {
    conf = getOzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.LEGACY.name());
    conf.setBoolean(OMConfigKeys.OZONE_OM_KEY_PATH_LOCK_ENABLED, true);
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, false);
    conf.setInt(OzoneConfigKeys.OM_NUM_CONCURRENT_WRITE_THREADS_KEY, 1);
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();
    cluster.waitTobeOutOfSafeMode();

    store = OzoneClientFactory.getRpcClient(conf).getObjectStore();
  }

  protected OzoneConfiguration getOzoneConfiguration() {
    return new OzoneConfiguration();
  }

  @Test
  public void testOmBucketReadWriteFileOps() throws Exception {
    try {
      startCluster();
      FileOutputStream out = FileUtils.openOutputStream(new File(path,
          "conf"));
      cluster.getConf().writeXml(out);
      out.getFD().sync();
      out.close();

      verifyFreonCommand(new ParameterBuilder().setTotalThreadCount(200)
          .setNumOfReadOperations(0).setNumOfWriteOperations(1)
          .setFileCountForRead(0).setFileCountForWrite(100).setFileSizeInBytes(0)
          .setReadThreadPercentage(50));

    } finally {
      shutdown();
    }
  }

  private void verifyFreonCommand(ParameterBuilder parameterBuilder)
      throws IOException {
    store.createVolume(parameterBuilder.volumeName);
    OzoneVolume volume = store.getVolume(parameterBuilder.volumeName);
    volume.createBucket(parameterBuilder.bucketName);
    String rootPath = "o3fs://" + parameterBuilder.bucketName + "." +
            parameterBuilder.volumeName + parameterBuilder.prefixFilePath;
    String confPath = new File(path, "conf").getAbsolutePath();

    long startTime = System.currentTimeMillis();

    new Freon().execute(
        new String[]{"-conf", confPath, "obrwf", "-P", rootPath,
            "-r", String.valueOf(parameterBuilder.fileCountForRead),
            "-w", String.valueOf(parameterBuilder.fileCountForWrite),
            "-g", String.valueOf(parameterBuilder.fileSizeInBytes),
            "-b", String.valueOf(parameterBuilder.bufferSize),
            "-l", String.valueOf(parameterBuilder.length),
            "-c", String.valueOf(parameterBuilder.totalThreadCount),
            "-T", String.valueOf(parameterBuilder.readThreadPercentage),
            "-R", String.valueOf(parameterBuilder.numOfReadOperations),
            "-W", String.valueOf(parameterBuilder.numOfWriteOperations),
            "-n", String.valueOf(1)});

    long totalTime = System.currentTimeMillis() - startTime;
    LOG.info("Total Time Taken: " + totalTime);

    verifyOMLockMetrics(cluster.getOzoneManager().getMetadataManager().getLock()
        .getOMLockMetrics());

  }

  private void verifyOMLockMetrics(OMLockMetrics omLockMetrics) {
    String readLockWaitingTimeMsStat =
        omLockMetrics.getReadLockWaitingTimeMsStat();
    LOG.info("Read Lock Waiting Time Stat: " + readLockWaitingTimeMsStat);
    LOG.info("Longest Read Lock Waiting Time (ms): " +
        omLockMetrics.getLongestReadLockWaitingTimeMs());
    int readWaitingSamples =
        Integer.parseInt(readLockWaitingTimeMsStat.split(" ")[2]);
    Assert.assertTrue("Read Lock Waiting Samples should be positive",
        readWaitingSamples > 0);

    String readLockHeldTimeMsStat = omLockMetrics.getReadLockHeldTimeMsStat();
    LOG.info("Read Lock Held Time Stat: " + readLockHeldTimeMsStat);
    LOG.info("Longest Read Lock Held Time (ms): " +
        omLockMetrics.getLongestReadLockHeldTimeMs());
    int readHeldSamples =
        Integer.parseInt(readLockHeldTimeMsStat.split(" ")[2]);
    Assert.assertTrue("Read Lock Held Samples should be positive",
        readHeldSamples > 0);

    String writeLockWaitingTimeMsStat =
        omLockMetrics.getWriteLockWaitingTimeMsStat();
    LOG.info("Write Lock Waiting Time Stat: " + writeLockWaitingTimeMsStat);
    LOG.info("Longest Write Lock Waiting Time (ms): " +
        omLockMetrics.getLongestWriteLockWaitingTimeMs());
    int writeWaitingSamples =
        Integer.parseInt(writeLockWaitingTimeMsStat.split(" ")[2]);
    Assert.assertTrue("Write Lock Waiting Samples should be positive",
        writeWaitingSamples > 0);

    String writeLockHeldTimeMsStat = omLockMetrics.getWriteLockHeldTimeMsStat();
    LOG.info("Write Lock Held Time Stat: " + writeLockHeldTimeMsStat);
    LOG.info("Longest Write Lock Held Time (ms): " +
        omLockMetrics.getLongestWriteLockHeldTimeMs());
    int writeHeldSamples =
        Integer.parseInt(writeLockHeldTimeMsStat.split(" ")[2]);
    Assert.assertTrue("Write Lock Held Samples should be positive",
        writeHeldSamples > 0);
  }

  private static class ParameterBuilder {

    private String volumeName = "vol1";
    private String bucketName = "bucket1";
    private String prefixFilePath = "/dir1/dir2";
    private int fileCountForRead = 100;
    private int fileCountForWrite = 10;
    private long fileSizeInBytes = 256;
    private int bufferSize = 64;
    private int length = 10;
    private int totalThreadCount = 100;
    private int readThreadPercentage = 90;
    private int numOfReadOperations = 50;
    private int numOfWriteOperations = 10;

    private ParameterBuilder setVolumeName(String volumeNameParam) {
      volumeName = volumeNameParam;
      return this;
    }

    private ParameterBuilder setBucketName(String bucketNameParam) {
      bucketName = bucketNameParam;
      return this;
    }

    private ParameterBuilder setPrefixFilePath(String prefixFilePathParam) {
      prefixFilePath = prefixFilePathParam;
      return this;
    }

    private ParameterBuilder setFileCountForRead(int fileCountForReadParam) {
      fileCountForRead = fileCountForReadParam;
      return this;
    }

    private ParameterBuilder setFileCountForWrite(int fileCountForWriteParam) {
      fileCountForWrite = fileCountForWriteParam;
      return this;
    }

    private ParameterBuilder setFileSizeInBytes(long fileSizeInBytesParam) {
      fileSizeInBytes = fileSizeInBytesParam;
      return this;
    }

    private ParameterBuilder setBufferSize(int bufferSizeParam) {
      bufferSize = bufferSizeParam;
      return this;
    }

    private ParameterBuilder setLength(int lengthParam) {
      length = lengthParam;
      return this;
    }

    private ParameterBuilder setTotalThreadCount(int totalThreadCountParam) {
      totalThreadCount = totalThreadCountParam;
      return this;
    }

    private ParameterBuilder setReadThreadPercentage(
        int readThreadPercentageParam) {
      readThreadPercentage = readThreadPercentageParam;
      return this;
    }

    private ParameterBuilder setNumOfReadOperations(
        int numOfReadOperationsParam) {
      numOfReadOperations = numOfReadOperationsParam;
      return this;
    }

    private ParameterBuilder setNumOfWriteOperations(
        int numOfWriteOperationsParam) {
      numOfWriteOperations = numOfWriteOperationsParam;
      return this;
    }
  }
}
