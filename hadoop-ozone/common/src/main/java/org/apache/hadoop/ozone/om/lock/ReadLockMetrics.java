package org.apache.hadoop.ozone.om.lock;

public class ReadLockMetrics {
    /*private String ThreadName;

    private long readLockWaitingStartTime;
    private long readLockHeldStartTime;

    private long readLockWaitingEndTime;
    private long readLockHeldEndTime;

    private long avgReadLockWaitingTime;
    private long avgReadLockHeldTime;*/

    private long readLockWaitingTime;
    private long readLockHeldTime;

    ReadLockMetrics(){
      readLockWaitingTime = -1;
      readLockHeldTime = -1;
    }

    ReadLockMetrics(long readLockWaitingTime, long readLockHeldTime){
      this.readLockWaitingTime = readLockWaitingTime;
      this.readLockHeldTime = readLockHeldTime;
    }

    public long getReadLockWaitingTime() {
      return readLockWaitingTime;
    }

    public long getReadLockHeldTime() {
      return readLockHeldTime;
    }

    public void setReadLockWaitingTime(long ReadLockWaitingTime){
      readLockWaitingTime = ReadLockWaitingTime;
    }

    public void setReadLockHeldTime(long ReadLockHeldTime){
      readLockHeldTime = ReadLockHeldTime;
    }
  };
