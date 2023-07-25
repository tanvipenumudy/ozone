package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.hdds.client.ReplicationConfig;

public class OmKeyInfoLight {

  private String volumeName;
  private String bucketName;
  private String keyName;
  private long dataSize;
  private long creationTime;
  private long modificationTime;
  private ReplicationConfig replicationConfig;
  private boolean isFile;

  public OmKeyInfoLight(String volumeName, String bucketName, String keyName,
                        long dataSize, long creationTime, long modificationTime,
                        ReplicationConfig replicationConfig, boolean isFile) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.dataSize = dataSize;
    this.creationTime = creationTime;
    this.modificationTime = modificationTime;
    this.replicationConfig = replicationConfig;
    this.isFile = isFile;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public String getKeyName() {
    return keyName;
  }

  public void setKeyName(String keyName) {
    this.keyName = keyName;
  }

  public long getDataSize() {
    return dataSize;
  }

  public void setDataSize(long dataSize) {
    this.dataSize = dataSize;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(long creationTime) {
    this.creationTime = creationTime;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public void setModificationTime(long modificationTime) {
    this.modificationTime = modificationTime;
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  public void setReplicationConfig(ReplicationConfig replicationConfig) {
    this.replicationConfig = replicationConfig;
  }

  public boolean isFile() {
    return isFile;
  }

  public void setFile(boolean file) {
    isFile = file;
  }
}

