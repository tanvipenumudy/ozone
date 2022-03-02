package org.apache.hadoop.ozone.om.lock;

public class WriteLockMetrics {
    /*private String ThreadName;

    private long writeLockWaitingStartTime;
    private long writeLockHeldStartTime;

    private long writeLockWaitingEndTime;
    private long writeLockHeldEndTime;

    private long avgWriteLockWaitingTime;
    private long avgWriteLockHeldTime;*/

    private long writeLockWaitingTime;
    //private long writeLockHeldTime;

    WriteLockMetrics(){
      writeLockWaitingTime = -1;
      //writeLockHeldTime = -1;
    }

    WriteLockMetrics(long writeLockWaitingTime){
      this.writeLockWaitingTime = writeLockWaitingTime;
      //this.writeLockHeldTime = writeLockHeldTime;
    }

    public long getWriteLockWaitingTime() {
      return writeLockWaitingTime;
    }

    /*public long getWriteLockHeldTime() {
      return writeLockHeldTime;
    }*/

    public void setWriteLockWaitingTime(long WriteLockWaitingTime){
      writeLockWaitingTime = WriteLockWaitingTime;
    }

    /*public void setWriteLockHeldTime(long WriteLockHeldTime){
      writeLockHeldTime = WriteLockHeldTime;
    }*/
  };
