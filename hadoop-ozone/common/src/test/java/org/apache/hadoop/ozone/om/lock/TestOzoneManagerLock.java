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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Class tests OzoneManagerLock.
 */
public class TestOzoneManagerLock {
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
    } else {
      return new String[]{UUID.randomUUID().toString()};
    }
  }

  private String generateResourceLockName(OzoneManagerLock.Resource resource,
                                          String... resources) {
    if (resources.length == 1 &&
        resource != OzoneManagerLock.Resource.BUCKET_LOCK) {
      return OzoneManagerLockUtil.generateResourceLockName(resource,
          resources[0]);
    } else if (resources.length == 2 &&
        resource == OzoneManagerLock.Resource.BUCKET_LOCK) {
      return OzoneManagerLockUtil.generateBucketLockName(resources[0],
          resources[1]);
    } else {
      throw new IllegalArgumentException("acquire lock is supported on single" +
          " resource for all locks except for resource bucket");
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
  public void testLockHoldCount() throws Exception {
    String[] resourceName;
    String resourceLockName;
    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      // USER_LOCK, S3_SECRET_LOCK and PREFIX_LOCK disallow lock re-acquire by
      // the same thread.
      if (resource == OzoneManagerLock.Resource.BUCKET_LOCK) {
        resourceName = generateResourceName(resource);
        resourceLockName = generateResourceLockName(resource, resourceName);
        testLockHoldCountUtil(resource, resourceName, resourceLockName);
      }
    }
  }

  ReentrantReadWriteLock coarseLock = new ReentrantReadWriteLock();

  @Test
  public void sampleTest() {
    testLock();
    testLock();
    testLock();
    testLock();
    testLock();
    testUnlock();
    testUnlock();
    testUnlock();
    testUnlock();
    testUnlock();
  }

  @Test
  public void sampleTest2() throws InterruptedException {

    final int threadCount = 5;
    Thread[] threads = new Thread[threadCount];
    System.out.println(coarseLock.getReadLockCount());
    for (int i = 0; i < threads.length; i++) {
      int finalI = i;
      threads[i] = new Thread(() -> {
        testLock();
        System.out.println("Lock " + coarseLock.getReadLockCount() + " " + Thread.currentThread().getName());
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        testUnlock();
        System.out.println("Unlock " + coarseLock.getReadLockCount() + " " + Thread.currentThread().getName());
      });
      threads[i].start();
    }
    for (Thread t : threads) {
      t.join();
    }
    System.out.println(coarseLock.getReadLockCount());
  }


  public void testLock() {
    coarseLock.readLock().lock();
    int count = coarseLock.getReadLockCount();
    //System.out.println("count = " + count);
  }

  public void testUnlock() {
    coarseLock.readLock().unlock();
    int count = coarseLock.getReadLockCount();
    //System.out.println("count = " + count);
  }

  private void testLockHoldCountUtil(OzoneManagerLock.Resource resource,
                                     String[] resourceName,
                                     String resourceLockName) {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    lock.acquireWriteLock(resource, resourceName);
    lock.acquireWriteLock(resource, resourceName);
    lock.acquireReadLock(resource, resourceName);
    lock.acquireReadLock(resource, resourceName);
    lock.acquireReadLock(resource, resourceName);
    lock.acquireReadLock(resource, resourceName);
    lock.releaseReadLock(resource, resourceName);
    lock.releaseReadLock(resource, resourceName);
    lock.releaseReadLock(resource, resourceName);
    lock.releaseReadLock(resource, resourceName);
    lock.releaseWriteLock(resource, resourceName);
    lock.releaseWriteLock(resource, resourceName);

    System.out.println(
        "getReadLockHeldTimeMsStat() -> " + lock.getReadLockHeldTimeMsStat());
    System.out.println("getReadLockWaitingTimeMsStat() -> " +
        lock.getReadLockWaitingTimeMsStat());

    System.out.println(
        "getWriteLockHeldTimeMsStat() -> " + lock.getWriteLockHeldTimeMsStat());
    System.out.println("getWriteLockWaitingTimeMsStat() -> " +
        lock.getWriteLockWaitingTimeMsStat());

    assertEquals(0, lock.getHoldCount(resourceLockName));

    for (int i = 1; i <= 5; i++) {
      lock.acquireReadLock(resource, resourceName);
      assertEquals(i, lock.getHoldCount(resourceLockName));
    }

    for (int i = 4; i >= 0; i--) {
      lock.releaseReadLock(resource, resourceName);
      assertEquals(i, lock.getHoldCount(resourceLockName));
    }

    for (int i = 1; i <= 5; i++) {
      lock.acquireWriteLock(resource, resourceName);
      assertEquals(i, lock.getHoldCount(resourceLockName));
    }

    for (int i = 4; i >= 0; i--) {
      lock.releaseWriteLock(resource, resourceName);
      assertEquals(i, lock.getHoldCount(resourceLockName));
    }
  }

  @Test
  public void testReadLockConcurrentStats() throws InterruptedException {
    // GIVEN
    final int threadCount = 5;
    OzoneManagerLock.Resource resource = OzoneManagerLock.Resource.BUCKET_LOCK;
    String[] resourceName = generateResourceName(resource);
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    Thread[] threads = new Thread[threadCount];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
        lock.acquireReadLock(resource, resourceName);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        lock.releaseReadLock(resource, resourceName);
      });
      threads[i].start();
    }
    for (Thread t : threads) {
      t.join();
    }

    // WHEN
    String heldStat = lock.getReadLockHeldTimeMsStat();

/*    System.out.println(
        "getReadLockHeldTimeMsStat() -> " + lock.getReadLockHeldTimeMsStat());
    System.out.println("getReadLockWaitingTimeMsStat() -> " +
        lock.getReadLockWaitingTimeMsStat());
    System.out.println("getLongestReadLockHeldTimeMs() -> " +
        lock.getLongestReadLockHeldTimeMs());
    System.out.println("getLongestReadLockWaitingTimeMs() -> " +
        lock.getLongestReadLockWaitingTimeMs());*/

    // THEN
    /*Assert.assertTrue("Expected " + threadCount + " samples in " + heldStat,
        heldStat.contains("Samples = " + threadCount));*/
  }
}
