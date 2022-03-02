package org.apache.hadoop.ozone.om.lock;

public class LockUsageInfo {

  private long readLockWaitingTime;
  private long readLockHeldTime;

  LockUsageInfo() {
    readLockWaitingTime = -1;
    readLockHeldTime = -1;
  }

  LockUsageInfo(long readLockWaitingTime, long readLockHeldTime) {
    this.readLockWaitingTime = readLockWaitingTime;
    this.readLockHeldTime = readLockHeldTime;
  }

  public long getReadLockWaitingTime() {
    return readLockWaitingTime;
  }

  public long getReadLockHeldTime() {
    return readLockHeldTime;
  }

  public void setReadLockWaitingTime(long readLockWaitingTimeNanos) {
    if (readLockWaitingTime < readLockWaitingTimeNanos) {
      readLockWaitingTime = readLockWaitingTimeNanos;
    }
  }

  public void setReadLockHeldTime(long readLockHeldTimeNanos) {
    if (readLockHeldTime < readLockHeldTimeNanos) {
      readLockHeldTime = readLockHeldTimeNanos;
    }
  }
};
