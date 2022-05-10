package org.apache.hadoop.ozone.freon;

import java.util.concurrent.Callable;

public abstract class AbstractOmBucketReadWriteOps extends BaseFreonGenerator
    implements Callable<Void> {

  public abstract int readOperations() throws Exception;

  public abstract int writeOperations() throws Exception;

  public abstract void create(String path) throws Exception;
}
