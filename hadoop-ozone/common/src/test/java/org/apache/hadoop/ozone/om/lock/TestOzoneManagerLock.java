/**
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

package org.apache.hadoop.ozone.om.lock;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.fail;

/**
 * Class tests OzoneManagerLock.
 */
public class TestOzoneManagerLock {

  @Rule
  public Timeout timeout = Timeout.seconds(100);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneManagerLock.class);

  @Test
  public void acquireResourceLock() {
    String[] resourceName;
    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      resourceName = generateResourceName(resource);
      testResourceLock(resourceName, resource);
    }
  }

  private void testResourceLock(String[] resourceName,
      OzoneManagerLock.Resource resource) {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireWriteLock(resource, resourceName);
    lock.releaseWriteLock(resource, resourceName);
    Assert.assertTrue(true);
  }

  @Test
  public void reacquireResourceLock() {
    String[] resourceName;
    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      resourceName = generateResourceName(resource);
      testResourceReacquireLock(resourceName, resource);
    }
  }

  private void testResourceReacquireLock(String[] resourceName,
      OzoneManagerLock.Resource resource) {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    // Lock re-acquire not allowed by same thread.
    if (resource == OzoneManagerLock.Resource.USER_LOCK ||
        resource == OzoneManagerLock.Resource.S3_SECRET_LOCK ||
        resource == OzoneManagerLock.Resource.PREFIX_LOCK) {
      lock.acquireWriteLock(resource, resourceName);
      try {
        lock.acquireWriteLock(resource, resourceName);
        fail("reacquireResourceLock failed");
      } catch (RuntimeException ex) {
        String message = "cannot acquire " + resource.getName() + " lock " +
            "while holding [" + resource.getName() + "] lock(s).";
        Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
      }
      lock.releaseWriteLock(resource, resourceName);
      Assert.assertTrue(true);
    } else {
      lock.acquireWriteLock(resource, resourceName);
      lock.acquireWriteLock(resource, resourceName);
      lock.releaseWriteLock(resource, resourceName);
      lock.releaseWriteLock(resource, resourceName);
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testLockingOrder() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String[] resourceName;

    // What this test does is iterate all resources. For each resource
    // acquire lock, and then in inner loop acquire all locks with higher
    // lock level, finally release the locks.
    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      Stack<ResourceInfo> stack = new Stack<>();
      resourceName = generateResourceName(resource);
      lock.acquireWriteLock(resource, resourceName);
      stack.push(new ResourceInfo(resourceName, resource));
      for (OzoneManagerLock.Resource higherResource :
          OzoneManagerLock.Resource.values()) {
        if (higherResource.getMask() > resource.getMask()) {
          resourceName = generateResourceName(higherResource);
          lock.acquireWriteLock(higherResource, resourceName);
          stack.push(new ResourceInfo(resourceName, higherResource));
        }
      }
      // Now release locks
      while (!stack.empty()) {
        ResourceInfo resourceInfo = stack.pop();
        lock.releaseWriteLock(resourceInfo.getResource(),
            resourceInfo.getLockName());
      }
    }
    Assert.assertTrue(true);
  }

  @Test
  public void testLockViolationsWithOneHigherLevelLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      for (OzoneManagerLock.Resource higherResource :
          OzoneManagerLock.Resource.values()) {
        if (higherResource.getMask() > resource.getMask()) {
          String[] resourceName = generateResourceName(higherResource);
          lock.acquireWriteLock(higherResource, resourceName);
          try {
            lock.acquireWriteLock(resource, generateResourceName(resource));
            fail("testLockViolationsWithOneHigherLevelLock failed");
          } catch (RuntimeException ex) {
            String message = "cannot acquire " + resource.getName() + " lock " +
                "while holding [" + higherResource.getName() + "] lock(s).";
            Assert.assertTrue(ex.getMessage(),
                ex.getMessage().contains(message));
          }
          lock.releaseWriteLock(higherResource, resourceName);
        }
      }
    }
  }

  @Test
  public void testLockViolations() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String[] resourceName;

    // What this test does is iterate all resources. For each resource
    // acquire an higher level lock above the resource, and then take the the
    // lock. This should fail. Like that it tries all error combinations.
    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      Stack<ResourceInfo> stack = new Stack<>();
      List<String> currentLocks = new ArrayList<>();
      for (OzoneManagerLock.Resource higherResource :
          OzoneManagerLock.Resource.values()) {
        if (higherResource.getMask() > resource.getMask()) {
          resourceName = generateResourceName(higherResource);
          lock.acquireWriteLock(higherResource, resourceName);
          stack.push(new ResourceInfo(resourceName, higherResource));
          currentLocks.add(higherResource.getName());
          // try to acquire lower level lock
          try {
            resourceName = generateResourceName(resource);
            lock.acquireWriteLock(resource, resourceName);
          } catch (RuntimeException ex) {
            String message = "cannot acquire " + resource.getName() + " lock " +
                "while holding " + currentLocks.toString() + " lock(s).";
            Assert.assertTrue(ex.getMessage(),
                ex.getMessage().contains(message));
          }
        }
      }

      // Now release locks
      while (!stack.empty()) {
        ResourceInfo resourceInfo = stack.pop();
        lock.releaseWriteLock(resourceInfo.getResource(),
            resourceInfo.getLockName());
      }
    }
  }

  @Test
  public void releaseLockWithOutAcquiringLock() {
    OzoneManagerLock lock =
        new OzoneManagerLock(new OzoneConfiguration());
    try {
      lock.releaseWriteLock(OzoneManagerLock.Resource.USER_LOCK, "user3");
      fail("releaseLockWithOutAcquiringLock failed");
    } catch (IllegalMonitorStateException ex) {
      String message = "Releasing lock on resource $user3 without acquiring " +
          "lock";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
  }


  private String[] generateResourceName(OzoneManagerLock.Resource resource) {
    if (resource == OzoneManagerLock.Resource.BUCKET_LOCK) {
      return new String[]{UUID.randomUUID().toString(),
          UUID.randomUUID().toString()};
    } else if (resource == OzoneManagerLock.Resource.KEY_LOCK) {
      return new String[]{UUID.randomUUID().toString(),
          UUID.randomUUID().toString(), UUID.randomUUID().toString()};
    } else {
      return new String[]{UUID.randomUUID().toString()};
    }
  }


  /**
   * Class used to store locked resource info.
   */
  public static class ResourceInfo {
    private String[] lockName;
    private OzoneManagerLock.Resource resource;

    ResourceInfo(String[] resourceName, OzoneManagerLock.Resource resource) {
      this.lockName = resourceName;
      this.resource = resource;
    }

    public String[] getLockName() {
      return lockName.clone();
    }

    public OzoneManagerLock.Resource getResource() {
      return resource;
    }
  }

  @Test
  public void acquireMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireMultiUserLock("user1", "user2");
    lock.releaseMultiUserLock("user1", "user2");
    Assert.assertTrue(true);
  }

  @Test
  public void reAcquireMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireMultiUserLock("user1", "user2");
    try {
      lock.acquireMultiUserLock("user1", "user2");
      fail("reAcquireMultiUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire USER_LOCK lock while holding " +
          "[USER_LOCK] lock(s).";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
    lock.releaseMultiUserLock("user1", "user2");
  }

  @Test
  public void acquireMultiUserLockAfterUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireWriteLock(OzoneManagerLock.Resource.USER_LOCK, "user3");
    try {
      lock.acquireMultiUserLock("user1", "user2");
      fail("acquireMultiUserLockAfterUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire USER_LOCK lock while holding " +
          "[USER_LOCK] lock(s).";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
    lock.releaseWriteLock(OzoneManagerLock.Resource.USER_LOCK, "user3");
  }

  @Test
  public void acquireUserLockAfterMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireMultiUserLock("user1", "user2");
    try {
      lock.acquireWriteLock(OzoneManagerLock.Resource.USER_LOCK, "user3");
      fail("acquireUserLockAfterMultiUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire USER_LOCK lock while holding " +
          "[USER_LOCK] lock(s).";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
    lock.releaseMultiUserLock("user1", "user2");
  }

  @Test
  public void testLockResourceParallel() throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      final String[] resourceName = generateResourceName(resource);
      lock.acquireWriteLock(resource, resourceName);

      AtomicBoolean gotLock = new AtomicBoolean(false);
      new Thread(() -> {
        lock.acquireWriteLock(resource, resourceName);
        gotLock.set(true);
        lock.releaseWriteLock(resource, resourceName);
      }).start();
      // Let's give some time for the new thread to run
      Thread.sleep(100);
      // Since the new thread is trying to get lock on same resource,
      // it will wait.
      Assert.assertFalse(gotLock.get());
      lock.releaseWriteLock(resource, resourceName);
      // Since we have released the lock, the new thread should have the lock
      // now.
      // Let's give some time for the new thread to run
      Thread.sleep(100);
      Assert.assertTrue(gotLock.get());
    }

  }

  @Test
  public void testMultiLockResourceParallel() throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireMultiUserLock("user2", "user1");

    AtomicBoolean gotLock = new AtomicBoolean(false);
    new Thread(() -> {
      lock.acquireMultiUserLock("user1", "user2");
      gotLock.set(true);
      lock.releaseMultiUserLock("user1", "user2");
    }).start();
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    // Since the new thread is trying to get lock on same resource, it will
    // wait.
    Assert.assertFalse(gotLock.get());
    lock.releaseMultiUserLock("user2", "user1");
    // Since we have released the lock, the new thread should have the lock
    // now.
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    Assert.assertTrue(gotLock.get());
  }

  @Test
  public void testKeyPrefixLockMultiThreading() throws Exception {

    testSameKeyPathWriteLockWithMultiThreading(100, 1000); // 50, 500/1000
    testDiffKeyPathWriteLockWithMultiThreading(100, 100); // 50, 500/1000
    testKeyLockCase11(50, 100);
    testKeyLockCase3(50, 100);
    // Concurrent thread operations:
    // case-1 : Read only with same key Path - All are ALLOWED
    // case-2 : Read only with diff key Path - All are ALLOWED
    // case-3 : Read and Write on diff key Path - All are ALLOWED
    // case-4 : Read and Write on same key Path - 2nd op should be BLOCKED
    // case-5 : Write and Read on same key Path - 2nd op should be BLOCKED
    // case-6 : Write and Read on diff key Path - All are ALLOWED
    // case-7 : Acquired Read Lock on KeyPath and trying to acquire a Read Bucket Lock on the same Key's bucket.
    // For ex: Read Lock = /vol1/buck1/a/b/c/d/key1 and then try to acquire a Read Bucket Lock on "/vol1/buck1" - All are ALLOWED
    // case-8 : Acquired Read Lock on KeyPath and trying to acquire a Write Bucket Lock on the same Key's bucket.
    // For ex: Read Lock = /vol1/buck1/a/b/c/d/key1 and then try to acquire a Write Bucket Lock on "/vol1/buck1" - and op should be BLOCKED
    // case-9 : Acquired Read Lock on KeyPath and trying to acquire a Write Bucket Lock on the same Key's bucket.
    // For ex: Read Lock = /vol1/buck1/a/b/c/d/key1 and then try to acquire a Write Bucket Lock on "/vol1/buck1" - and op should be BLOCKED
    // case-10 : Acquired Write Lock on KeyPath and trying to acquire a Read Bucket Lock on the same Key's bucket.
    // For ex: Write Lock = /vol1/buck1/a/b/c/d/key1 and then try to acquire a Read Bucket Lock on "/vol1/buck1" - All are ALLOWED
    // case-11 : Acquired Write Lock on KeyPath and trying to acquire a Write Bucket Lock on the same Key's bucket.
    // For ex: Write Lock = /vol1/buck1/a/b/c/d/key1 and then try to acquire a Write Bucket Lock on "/vol1/buck1" -  op should be BLOCKED

    // Reenterant operations by same thread cases:


    //Your Question: How do we proceed with the changes in community? am I correct?
    // We will split the task into multiple sub-tasks and then prioritize similar to the metrics patch.
    // We will create an umbrella jira and create smaller sub-tasks.

  }

  class Counter {
    private int count = 0;

    public void incrementCount() {
      count++;
    }

    public int getCount() {
      return count;
    }
  }

  // test-case-1
  // "/a/b/c/d/key1 - WLock - 1st iteration"
  // "/a/b/c/d/key1 - WLock - 2nd iteration"
  // "/a/b/c/d/key1 - WLock - 3rd iteration"
  // "/a/b/c/d/key1 - WLock - 4th iteration"
  // "/a/b/c/d/key1 - WLock - 5th iteration"  -- blocked

  // Expected Time Elapsed: >=5000 ms (since the iterations are sequential)

  public void testSameKeyPathWriteLockWithMultiThreading(int threadCount, int iterations)
      throws InterruptedException {

    OzoneManagerLock.Resource resource =
        OzoneManagerLock.Resource.KEY_LOCK;

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    Thread[] threads = new Thread[threadCount];
    Counter counter = new Counter();
    CountDownLatch countDownLatch = new CountDownLatch(threadCount);
    List<Integer> listTokens = new ArrayList<>(threadCount);

    for (int i = 0; i < threads.length; i++) {

      threads[i] = new Thread(() -> {
        String[] sampleResourceName =
            new String[]{volumeName, bucketName, keyName};

        // Waiting all threads to be instantiated and reached to the acquireWriteLock.
        countDownLatch.countDown();
        while (countDownLatch.getCount() > 0) {
          try {
            Thread.sleep(500);
            LOG.info("countDown.getCount() -> " + countDownLatch.getCount());
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        // Now, all threads have been instantiated.
        Assert.assertEquals(0, countDownLatch.getCount());

        lock.acquireWriteLock(resource, sampleResourceName);
        LOG.info("Write Lock Acquired by " + Thread.currentThread().getName());

        /**
         * Critical Section. count = count + 1;
         */
        for (int idx = 0; idx < iterations; idx++) {
          counter.incrementCount();
        }
        /* Sequence of tokens ranging from 1-100 for each thread.
        For example:
         Thread-1 -> 1-100
         Thread-2 -> 101-200 and so on.
         */
        listTokens.add(counter.getCount());

        lock.releaseWriteLock(resource, sampleResourceName);
        LOG.info("Write Lock Released by " + Thread.currentThread().getName());
      });

      threads[i].start();
    }

    // Waiting for all threads to finish the execution(run method).
    for (Thread t : threads) {
      t.join();
    }

    // For example, threadCount = 50, iterations = 100. Exoected counter value is 50 * 100
    Assert.assertEquals(threadCount * iterations, counter.getCount());
    Assert.assertEquals(threadCount, listTokens.size());

    /**
     * Thread-1 = 100,
     * Thread-2 = 200 and so on.
     */
    for (int i = 1; i <= listTokens.size(); i++) {
      Assert.assertEquals((new Integer(i * iterations)), listTokens.get(i - 1));
    }
  }

  // test-case-2
  // "/a/b/c/d/key1 - WLock - 1st iteration"
  // "/a/b/c/d/key2 - WLock - 2nd iteration"
  // "/a/b/c/d/key3 - WLock - 3rd iteration"
  // "/a/b/c/d/key4 - WLock - 4th iteration"
  // "/a/b/c/d/key5 - WLock - 5th iteration"  -- allowed

  // Expected Time Elapsed: >=1000 ms (since the iterations are parallel)

  public void testDiffKeyPathWriteLockWithMultiThreading(int threadCount,
                                                         int iterations)
      throws Exception {

    OzoneManagerLock.Resource resource =
        OzoneManagerLock.Resource.KEY_LOCK;

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    Thread[] threads = new Thread[threadCount];
    Counter counter = new Counter();
    CountDownLatch countDown = new CountDownLatch(threadCount);

    for (int i = 0; i < threads.length; i++) {

      threads[i] = new Thread(() -> {
        String keyName = UUID.randomUUID().toString();
        String[] sampleResourceName =
            new String[]{volumeName, bucketName, keyName};

        lock.acquireWriteLock(resource, sampleResourceName);
        LOG.info("Write Lock Acquired by " + Thread.currentThread().getName());

        countDown.countDown();
        while (countDown.getCount() > 0) {
          try {
            Thread.sleep(500);
            LOG.info("countDown.getCount() -> " + countDown.getCount());
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        Assert.assertEquals(1, lock.getCurrentLocks().size());

        lock.releaseWriteLock(resource, sampleResourceName);
        LOG.info("Write Lock Released by " + Thread.currentThread().getName());
      });

      threads[i].start();
    }

    /**
     * Waiting for all the threads to count down
     */
    GenericTestUtils.waitFor(() -> {
          if (countDown.getCount() > 0) {
            LOG.info("Waiting for the trheads to count down {} ",
                countDown.getCount());
            return false;
          }
          return true; // all thrads has finished counting down.
        }, 3000,
        120000); // 2 minutes

    Assert.assertEquals(0, countDown.getCount());

    for (Thread t : threads) {
      t.join();
    }

    LOG.info("Expected=" + threadCount * iterations + ", Actual=" +
        counter.getCount());
  }

  public void testKeyLockCase11(int threadCount, int iterations)
      throws InterruptedException {

    System.out.println("--------Test Case 3--------");

    OzoneManagerLock.Resource resource =
        OzoneManagerLock.Resource.KEY_LOCK;

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    Thread[] threads = new Thread[threadCount];
    Counter counter = new Counter();

    for (int i = 0; i < threads.length; i++) {

      int finalI = i;
      threads[i] = new Thread(() -> {
        String[] sampleResourceName =
            new String[]{volumeName, bucketName, keyName};

        lock.acquireWriteLock(resource, sampleResourceName);
        LOG.info("Write Lock Acquired by " + Thread.currentThread().getName());

//        System.out.println("I " + finalI + " " + Thread.activeCount());

        /**
         * Critical Section. count = count + 1;
         */
        for (int idx = 0; idx < iterations; idx++) {
          counter.incrementCount();
        }

        lock.releaseWriteLock(resource, sampleResourceName);
        LOG.info("Write Lock Released by " + Thread.currentThread().getName());
      });

      threads[i].start();
    }

    for (Thread t : threads) {
      t.join();
    }

    Assert.assertEquals(threadCount * iterations, counter.getCount());
  }

  // test-case-3
  // "/a/b/c/d/key1 - RLock - 1st iteration"
  // "/a/b/c/d/key1 - RLock - 2nd iteration"
  // "/a/b/c/d/key1 - RLock - 3rd iteration"
  // "/a/b/c/d/key1 - RLock - 4th iteration"
  // "/a/b/c/d/key2 - WLock - 1st iteration" -- allowed

  // Expected Time Elapsed: >=1000 ms (since the RLock iterations are parallel,
  // WLock iteration is on a different key path)

  public void testKeyLockCase3(int threadCount, int iterations) throws InterruptedException {

    OzoneManagerLock.Resource resource =
        OzoneManagerLock.Resource.KEY_LOCK;

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    Thread[] threads = new Thread[threadCount];
    Counter counter = new Counter();
    CountDownLatch countDownLatch = new CountDownLatch(threadCount-1);
    List<Integer> listTokens = new ArrayList<>(threadCount);

    for (int i = 0; i < threads.length-1; i++) {

      threads[i] = new Thread(() -> {
        String[] sampleResourceName =
            new String[]{volumeName, bucketName, keyName};

        countDownLatch.countDown();
        while (countDownLatch.getCount() > 0) {
          try {
            Thread.sleep(500);
            LOG.info("countDown.getCount() -> " + countDownLatch.getCount());
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        // Now, all threads have been instantiated.
        Assert.assertEquals(0, countDownLatch.getCount());

        lock.acquireReadLock(resource, sampleResourceName);
        LOG.info("Read Lock Acquired by " + Thread.currentThread().getName());

        /**
         * Critical Section. count = count + 1;
         */
        for (int idx = 0; idx < iterations; idx++) {
          counter.incrementCount();
        }
        listTokens.add(counter.getCount());

        lock.releaseReadLock(resource, sampleResourceName);
        LOG.info("Read Lock Released by " + Thread.currentThread().getName());
      });

      threads[i].start();
    }

    threads[threads.length-1] = new Thread(() -> {
      String[] sampleResourceName =
          new String[]{volumeName, bucketName, UUID.randomUUID().toString()};
      lock.acquireWriteLock(resource, sampleResourceName);
        LOG.info("Write Lock Acquired by " + Thread.currentThread().getName());
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      lock.releaseWriteLock(resource, sampleResourceName);
        LOG.info("Write Lock Released by " + Thread.currentThread().getName());
    });
    threads[threads.length-1].start();

    for (Thread t : threads) {
      t.join();
    }

    System.out.println(threadCount-1 + " " + listTokens.size());

    for (int i = 1; i <= listTokens.size(); i++) {
      System.out.println((new Integer(i * iterations)) + " " + listTokens.get(i - 1));
    }
  }

  // test-case-4
  // "/a/b/c/d/key1 - RLock - 1st iteration"
  // "/a/b/c/d/key1 - RLock - 2nd iteration"
  // "/a/b/c/d/key1 - RLock - 3rd iteration"
  // "/a/b/c/d/key1 - RLock - 4th iteration"
  // "/a/b/c/d/key1 - WLock - 1st iteration" -- blocked

  // Expected Time Elapsed: >=2000/3000 ms (since the RLock iterations are parallel,
  // WLock iteration is sequential on the same key path)

  public long testKeyLockCase4() throws InterruptedException {

    System.out.println("\n---------Test Case: 4---------");
    final int threadCount = 5;

    OzoneManagerLock.Resource resource =
        OzoneManagerLock.Resource.KEY_LOCK;

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    Thread[] threads = new Thread[threadCount];
    long startTime = Time.monotonicNow();

    for (int i = 0; i < threads.length-1; i++) {

      threads[i] = new Thread(() -> {
        String[] sampleResourceName =
            new String[]{volumeName, bucketName, keyName};
        lock.acquireReadLock(resource, sampleResourceName);
        System.out.println(
            "Read Lock Acquired by " + Thread.currentThread().getName());
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        lock.releaseReadLock(resource, sampleResourceName);
        System.out.println(
            "Read Lock Released by " + Thread.currentThread().getName());
      });
      threads[i].start();
    }

    threads[threads.length-1] = new Thread(() -> {
      String[] sampleResourceName =
          new String[]{volumeName, bucketName, keyName};
      lock.acquireWriteLock(resource, sampleResourceName);
      System.out.println(
          "Write Lock Acquired by " + Thread.currentThread().getName());
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      lock.releaseWriteLock(resource, sampleResourceName);
      System.out.println(
          "Write Lock Released by " + Thread.currentThread().getName());
    });
    threads[threads.length-1].start();

    for (Thread t : threads) {
      t.join();
    }

    long endTime = Time.monotonicNow() - startTime;
    System.out.println("Test Case-4 Time Elapsed: " + endTime);
    return endTime;
  }
}
