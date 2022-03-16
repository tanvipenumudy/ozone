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
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
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
 * Test for OmBucketReadWriteOps.
 */
public class TestOmBucketReadWriteOps {

  private String path;
  private OzoneConfiguration conf = null;
  private MiniOzoneCluster cluster = null;
  private ObjectStore store = null;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestOmBucketReadWriteOps.class);

  @Before
  public void setup() {
    path = GenericTestUtils
        .getTempPath(TestOmBucketReadWriteOps.class.getSimpleName());
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
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();
    cluster.waitTobeOutOfSafeMode();

    store = OzoneClientFactory.getRpcClient(conf).getObjectStore();
  }

  protected OzoneConfiguration getOzoneConfiguration() {
    return new OzoneConfiguration();
  }

  @Test
  public void testFileGeneration() throws Exception {
    try {
      startCluster();
      FileOutputStream out = FileUtils.openOutputStream(new File(path,
          "conf"));
      cluster.getConf().writeXml(out);
      out.getFD().sync();
      out.close();
      verifyReadWriteOps(
          new ParameterBuilder().setVolumeName("vol2").setBucketName("bucket2")
              .setPrefixFilePath("/dir1/dir2/dir3"));
    } finally {
      shutdown();
    }
  }

  private void verifyReadWriteOps(ParameterBuilder parameterBuilder)
      throws IOException {
    store.createVolume(parameterBuilder.volumeName);
    OzoneVolume volume = store.getVolume(parameterBuilder.volumeName);
    volume.createBucket(parameterBuilder.bucketName);
    String rootPath =
        "o3fs://" + parameterBuilder.volumeName + "." +
            parameterBuilder.bucketName + parameterBuilder.prefixFilePath;
    String confPath = new File(path, "conf").getAbsolutePath();
    new Freon().execute(
        new String[]{"-conf", confPath, "obrwo", "-P", rootPath,
            "-r", String.valueOf(parameterBuilder.fileCountForRead),
            "-w", String.valueOf(parameterBuilder.fileCountForWrite),
            "-g", String.valueOf(parameterBuilder.fileSizeInBytes),
            "-b", String.valueOf(parameterBuilder.bufferSize),
            "-l", String.valueOf(parameterBuilder.length),
            "-c", String.valueOf(parameterBuilder.totalThreadCount),
            "-T", String.valueOf(parameterBuilder.readThreadPercentage),
            "-R", String.valueOf(parameterBuilder.numOfReadOperations),
            "-W", String.valueOf(parameterBuilder.numOfWriteOperations),
            "-n", "1"});

    LOG.info("Started verifying read/write ops...");
    FileSystem fileSystem = FileSystem.get(URI.create(rootPath),
        conf);
    Path rootDir = new Path(rootPath.concat("/"));
    FileStatus[] fileStatuses = fileSystem.listStatus(rootDir);
    verify(2, fileStatuses, true);

    Path readDir = new Path(rootPath.concat("/readPath"));
    FileStatus[] readFileStatuses = fileSystem.listStatus(readDir);
    verify(parameterBuilder.fileCountForRead, readFileStatuses, false);

    Path writeDir = new Path(rootPath.concat("/writePath"));
    FileStatus[] writeFileStatuses = fileSystem.listStatus(writeDir);
    verify(parameterBuilder.numOfWriteOperations, writeFileStatuses, false);
  }

  private int verify(int expectedCount, FileStatus[] fileStatuses,
                     boolean checkDir) {
    int actual = 0;
    if (checkDir) {
      for (FileStatus fileStatus : fileStatuses) {
        if (fileStatus.isDirectory()) {
          ++actual;
        }
      }
    } else {
      for (FileStatus fileStatus : fileStatuses) {
        if (fileStatus.isFile()) {
          ++actual;
        }
      }
    }
    Assert.assertEquals("Mismatch Count!", expectedCount, actual);
    return actual;
  }

  private class ParameterBuilder {
    private String volumeName = "vol1";
    private String bucketName = "bucket1";
    private String prefixFilePath = "/dir1/dir2";
    private int fileCountForRead = 100;
    private int fileCountForWrite = 100;
    private long fileSizeInBytes = 256;
    private int bufferSize = 64;
    private int length = 10;
    private int totalThreadCount = 100;
    private int readThreadPercentage = 90;
    private int numOfReadOperations = 50;
    private int numOfWriteOperations = 50;

    /*public ParameterBuilder build() {
      return this;
    }*/

    private ParameterBuilder setVolumeName(String volumeName) {
      this.volumeName = volumeName;
      return this;
    }

    private ParameterBuilder setBucketName(String bucketName) {
      this.bucketName = bucketName;
      return this;
    }

    private ParameterBuilder setPrefixFilePath(String prefixFilePath) {
      this.prefixFilePath = prefixFilePath;
      return this;
    }

    private ParameterBuilder setFileCountForRead(int fileCountForRead) {
      this.fileCountForRead = fileCountForRead;
      return this;
    }

    private ParameterBuilder setFileCountForWrite(int fileCountForWrite) {
      this.fileCountForWrite = fileCountForWrite;
      return this;
    }

    private ParameterBuilder setFileSizeInBytes(long fileSizeInBytes) {
      this.fileSizeInBytes = fileSizeInBytes;
      return this;
    }

    private ParameterBuilder setBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    private ParameterBuilder setLength(int length) {
      this.length = length;
      return this;
    }

    private ParameterBuilder setTotalThreadCount(int totalThreadCount) {
      this.totalThreadCount = totalThreadCount;
      return this;
    }

    private ParameterBuilder setReadThreadPercentage(int readThreadPercentage) {
      this.readThreadPercentage = readThreadPercentage;
      return this;
    }

    private ParameterBuilder setNumOfReadOperations(int numOfReadOperations) {
      this.numOfReadOperations = numOfReadOperations;
      return this;
    }

    private ParameterBuilder setNumOfWriteOperations(int numOfWriteOperations) {
      this.numOfWriteOperations = numOfWriteOperations;
      return this;
    }
  }
}
