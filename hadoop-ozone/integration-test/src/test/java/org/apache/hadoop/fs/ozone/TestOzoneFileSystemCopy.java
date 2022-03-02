/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.ozone;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.util.Time;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Ozone file system tests that are not covered by contract tests.
 */
@RunWith(Parameterized.class)
public class TestOzoneFileSystemCopy implements Runnable{

  private static final float TRASH_INTERVAL = 0.05f; // 3 seconds


  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[]{true, true},
        new Object[]{true, false},
        new Object[]{false, true},
        new Object[]{false, false});
  }

  public TestOzoneFileSystemCopy(boolean setDefaultFs, boolean enableOMRatis) {
    // Checking whether 'defaultFS' and 'omRatis' flags represents next
    // parameter index values. This is to ensure that initialize
    // TestOzoneFileSystem#init() function will be invoked only at the
    // beginning of every new set of Parameterized.Parameters.
    if (enabledFileSystemPaths != setDefaultFs ||
            omRatisEnabled != enableOMRatis) {
      enabledFileSystemPaths = setDefaultFs;
      omRatisEnabled = enableOMRatis;
      try {
        teardown();
        init();
      } catch (Exception e) {
        LOG.info("Unexpected exception", e);
        fail("Unexpected exception:" + e.getMessage());
      }
    }
  }

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = Timeout.seconds(300);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneFileSystemCopy.class);

  private static BucketLayout bucketLayout = BucketLayout.LEGACY;
  private static boolean enabledFileSystemPaths;
  private static boolean omRatisEnabled;

  private static MiniOzoneCluster cluster;
  private static OzoneManagerProtocol writeClient;
  private static FileSystem fs;
  private static OzoneFileSystem o3fs;
  private static String volumeName;
  private static String bucketName;
  private static Trash trash;

  private void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setFloat(OMConfigKeys.OZONE_FS_TRASH_INTERVAL_KEY, TRASH_INTERVAL);
    conf.setFloat(FS_TRASH_INTERVAL_KEY, TRASH_INTERVAL);
    conf.setFloat(FS_TRASH_CHECKPOINT_INTERVAL_KEY, TRASH_INTERVAL / 2);

    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, omRatisEnabled);
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, false);
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    if (!bucketLayout.equals(BucketLayout.FILE_SYSTEM_OPTIMIZED)) {
      conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
          enabledFileSystemPaths);
    }
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        bucketLayout.name());
    cluster = MiniOzoneCluster.newBuilder(conf)
            .setNumDatanodes(3)
            .build();
    cluster.waitForClusterToBeReady();

    writeClient = cluster.getRpcClient().getObjectStore()
        .getClientProxy().getOzoneManagerClient();
    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket =
        TestDataUtil.createVolumeAndBucket(cluster, bucketLayout);
    volumeName = bucket.getVolumeName();
    bucketName = bucket.getName();

    String rootPath = String.format("%s://%s.%s/",
            OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName);

    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);

    fs = FileSystem.get(conf);
    trash = new Trash(conf);
    o3fs = (OzoneFileSystem) fs;
  }

  @AfterClass
  public static void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  @After
  public void cleanup() {
    try {
      Path root = new Path("/");
      FileStatus[] fileStatuses = fs.listStatus(root);
      for (FileStatus fileStatus : fileStatuses) {
        fs.delete(fileStatus.getPath(), true);
      }
    } catch (IOException ex) {
      fail("Failed to cleanup files.");
    }
  }

  public static MiniOzoneCluster getCluster() {
    return cluster;
  }

  public static FileSystem getFs() {
    return fs;
  }

  public static void setBucketLayout(BucketLayout bLayout) {
    bucketLayout = bLayout;
  }

  public static String getBucketName() {
    return bucketName;
  }

  public static String getVolumeName() {
    return volumeName;
  }

  public BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }

  @Test
  public void testSeekOnFileLength() throws IOException {
    Path file = new Path("/file");
    ContractTestUtils.createFile(fs, file, true, "a".getBytes(UTF_8));
    try (FSDataInputStream stream = fs.open(file)) {
      long fileLength = fs.getFileStatus(file).getLen();
      stream.seek(fileLength);
      assertEquals(-1, stream.read());
    }

    // non-existent file
    Path fileNotExists = new Path("/file_notexist");
    try {
      fs.open(fileNotExists);
      Assert.fail("Should throw FILE_NOT_FOUND error as file doesn't exist!");
    } catch (FileNotFoundException fnfe) {
      Assert.assertTrue("Expected FILE_NOT_FOUND error",
              fnfe.getMessage().contains("FILE_NOT_FOUND"));
    }
  }

  private long testCount;
  private String testData = "abcd";
  Object testLock = new Object();
  static Path file;

  @NotNull
  public Path createFile() throws IOException {
    // synchronized keyword ensures that only one thread can modify the count variable at a time
    Path parent = new Path("/dir1/dir2/dir3/dir4/");
    file = new Path(parent, "/file");
    //String testData = "abc";
    //long testDataLength = testData.length();
    ContractTestUtils.createFile(fs, file, true, testData.getBytes(UTF_8));
    return file;
  }

  public long getTestCount() {
    return testCount;
  }

  @Test
  public void testSeekOnFileLengthDemo() throws IOException {
    try (FSDataInputStream stream = fs.open(file)) {
      long fileLength = fs.getFileStatus(file).getLen();
      // waiting begin metrics
      synchronized (testLock) {
        // waiting end metrics
        // held begin metrics
        testLock.wait(100); // induced delay--waiting period.
        testCount = testCount + fileLength;
        // introduced a wait between the two steps within the method (delay between each thread execution)
      }
      // held end metrics

      stream.seek(fileLength);
      assertEquals(-1, stream.read());
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSeekOnFileLengthConcurrency()
      throws InterruptedException, IOException {
    file = createFile();
    int threadCount = 100;
    ExecutorService service = Executors.newFixedThreadPool(threadCount); //initialize number of threads
    CountDownLatch latch = new CountDownLatch(threadCount); //makes sure that a task waits for other threads before it starts
    //TestOzoneFileSystem testOzoneFileSystem = new TestOzoneFileSystem(true, true);
    for (int i = 0; i < threadCount; i++) { //iterate through number of tasks
      service.submit(() -> { //executes at some time in the future - in a new thread/pooled thread or based on executor implementation
        try {
          testSeekOnFileLengthDemo();
        } catch (Exception e) {
          e.printStackTrace();
        }
        latch.countDown();
      });
    }
    latch.await(); //causes current thread to wait until the latch has counted down to zero
    long testDataLength = testData.length();
    assertEquals(threadCount * testDataLength, getTestCount());
  }

  @Test
  public void testSeekOnFileLengthWithoutConcurrency()
      throws InterruptedException {
    for (int i = 0; i < 500; i++) {
      try {
        testSeekOnFileLengthDemo();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    assertEquals(500, getTestCount() / testData.length());
  }

  ThreadLocal<LockMetrics> threadLocal = new ThreadLocal(){
    protected LockMetrics initialValue() {
      LockMetrics lockMetrics = new LockMetrics();
      return lockMetrics;
    }
  };

  /*ThreadLocal<Long> threadLocal = new ThreadLocal(){
    protected Long initialValue() {
      return Long.valueOf(0);;
    }
  };*/

  @Override
  public void run(){

    try (FSDataInputStream stream = fs.open(file)) {
      long fileLength = fs.getFileStatus(file).getLen();
      // waiting begin metrics
      long lockWaitBegin = Time.monotonicNowNanos();
      long lockHeldBegin;
      //threadLocal.set(Long.valueOf(System.currentTimeMillis()));
      synchronized (testLock) {
        // waiting end metrics
        // held begin metrics
        //threadLocal.set(Long.valueOf(System.currentTimeMillis()) - threadLocal.get());
        threadLocal.get().setLockWaitingTime(Time.monotonicNowNanos() - lockWaitBegin);
        System.out.println(Thread.currentThread().getName() + " Waiting Time: " + threadLocal.get().getLockWaitingTime());
        lockHeldBegin = Time.monotonicNowNanos();
        testLock.wait(100);
        stream.seek(fileLength);
        //threadLocal.set(Long.valueOf(System.currentTimeMillis()));
        /*System.out.println(
            Thread.currentThread().getName() + " Waiting Time: " + threadLocal.get());*/
      }
      // held end metrics
      threadLocal.get().setLockHeldTime(System.currentTimeMillis() - lockHeldBegin);
      System.out.println(Thread.currentThread().getName() + " Held Time: " + threadLocal.get().getLockHeldTime());
      /*threadLocal.set(Long.valueOf(System.currentTimeMillis()) - threadLocal.get());
      System.out.println(
          Thread.currentThread().getName() + " Held Time: " + threadLocal.get());*/

    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    /*for(int i=0; i<5; i++) {
      threadLocal.set(threadLocal.get() + 1);
      System.out.println(
          Thread.currentThread().getName() + " count" + threadLocal.get());
    }*/
  }

  public static void main(String[] args) throws InterruptedException{
    TestOzoneFileSystemCopy runnable = new TestOzoneFileSystemCopy(true, false);

    try {
      file = runnable.createFile();
    } catch (IOException e) {
      e.printStackTrace();
    }

    ExecutorService service = Executors.newFixedThreadPool(1000);
    for (int i = 0; i < 1000; i++){
      service.execute(runnable);
    }

    /*Thread t1 = new Thread(runnable);
    Thread t2 = new Thread(runnable);
    Thread t3 = new Thread(runnable);

    t1.start();
    t2.start();
    t3.start();

    t1.join();
    t2.join();
    t3.join();*/

  }
}
