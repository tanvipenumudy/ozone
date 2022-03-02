package org.apache.hadoop.fs.ozone;

public class LockMetrics {
    /*private String ThreadName;
    private long lockWaitStartTime;
    private long lockHeldStartTime;
    private long lockWaitEndTime;
    private long lockHeldEndTime;*/

    private long lockWaitingTime;
    private long lockHeldTime;

    LockMetrics(){
      lockWaitingTime = -1;
      lockHeldTime = -1;
    }

    LockMetrics(long lockWaitingTime, long lockHeldTime){
      this.lockWaitingTime = lockWaitingTime;
      this.lockHeldTime = lockHeldTime;
    }

    public long getLockWaitingTime() {
      return lockWaitingTime;
    }

    public long getLockHeldTime() {
      return lockHeldTime;
    }

    public void setLockWaitingTime(long LockWaitingTime){
      lockWaitingTime = LockWaitingTime;
    }

    public void setLockHeldTime(long LockHeldTime){
      lockHeldTime = LockHeldTime;
    }
  };
