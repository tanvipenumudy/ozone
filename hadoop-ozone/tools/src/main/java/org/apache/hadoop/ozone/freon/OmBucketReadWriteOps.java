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

import com.codahale.metrics.Timer;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;

/**
 * Synthetic read/write operations workload generator tool.
 */
@Command(name = "obrwo",
    aliases = "om-bucket-read-write-ops",
    description = "Generate files, perform respective read/write operations " +
        "to measure lock performance, simulate lock contention.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)

public class OmBucketReadWriteOps extends BaseFreonGenerator
    implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(OmBucketReadWriteOps.class);

  @Option(names = {"-v", "--volume"},
      description = "Name of the volume which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "vol1")
  private String volumeName;

  @Option(names = {"-b", "--bucket"},
      description = "Name of the bucket which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "bucket1")
  private String bucketName;

  @Option(names = {"-pf", "prefix-file-path", "--prefixFilePath"},
      description = "Prefix file path. Full name --prefixFilePath will be " +
          "removed in later versions.",
      defaultValue = "/dir1/dir2")
  private String prefixFilePath;

  @Option(names = {"-rc", "--file-count-for-read", "--fileCountForRead"},
      description = "Number of files to be written in the read directory. " +
          "Full name --fileCountForRead will be removed in later versions.",
      defaultValue = "1000")
  private int fileCountForRead;

  @Option(names = {"-wc", "--file-count-for-write", "--fileCountForWrite"},
      description = "Number of files to be written in the write directory. " +
          "Full name --fileCountForWrite will be removed in later versions.",
      defaultValue = "1000")
  private int fileCountForWrite;

  @Option(names = {"-g", "--file-size", "--fileSize"},
      description = "Generated data size (in bytes) of each file to be " +
          "written in each directory. Full name --fileSize will be removed " +
          "in later versions.",
      defaultValue = "1024")
  private long fileSizeInBytes;

  // do we need a separate fileSizeInBytes for read and write?

  @Option(names = {"-b", "--buffer"},
      description = "Size of buffer used to generated the file content.",
      defaultValue = "256")
  private int bufferSize;

  @Option(names = {"-l", "--name-len", "--nameLen"},
      description =
          "Length of the random name of directory you want to create. Full " +
              "name --nameLen will be removed in later versions.",
      defaultValue = "10")
  private int length;

  @Option(names = {"-tc", "--total-thread-count", "--totalThreadCount"},
      description = "Total number of threads to be executed. Full name " +
          "--totalThreadCount will be removed in later versions.",
      defaultValue = "100")
  private int totalThreadCount;

  @Option(names = {"-rp", "--read-thread-percentage", "--readThreadPercentage"},
      description = "Percentage of the total number of threads to be " +
          "allocated for read operations. Full name --readThreadPercentage " +
          "will be removed in later versions.",
      defaultValue = "90")
  private int readThreadPercentage;

  @Option(names = {"-ro", "--num-of-read-operations", "--numOfReadOperations"},
      description = "Number of read operations to be performed by each " +
          "thread. Full name --numOfReadOperations will be removed in later " +
          "versions.",
      defaultValue = "50")
  private int numOfReadOperations;

  @Option(names = {"-wo", "--num-of-write-operations",
      "--numOfWriteOperations"},
      description = "Number of write operations to be performed by each " +
          "thread. Full name --numOfWriteOperations will be removed in later " +
          "versions.",
      defaultValue = "50")
  private int numOfWriteOperations;

  @Option(names = {"-F", "--factor"},
      description = "Replication factor (ONE, THREE)",
      defaultValue = "THREE"
  )
  private HddsProtos.ReplicationFactor factor =
      HddsProtos.ReplicationFactor.THREE;

  private Timer timer;

  private ContentGenerator contentGenerator;

  private FileSystem fileSystem;

  private KeyManagerImpl keyManager;

  private String prefixPath =
      "o3fs://" + bucketName + "." + volumeName + prefixFilePath;

  @Override
  public Void call() throws Exception {
    init();
    LOG.info("volumeName: " + volumeName);
    LOG.info("bucketName: " + bucketName);
    LOG.info("prefixFilePath: " + prefixFilePath);
    LOG.info("fileCountForRead: " + fileCountForRead);
    LOG.info("fileCountForWrite: " + fileCountForWrite);
    LOG.info("fileSizeInBytes: " + fileSizeInBytes);
    LOG.info("bufferSize: " + bufferSize);
    LOG.info("totalThreadCount: " + totalThreadCount);
    LOG.info("readThreadPercentage: " + readThreadPercentage);
    LOG.info("numOfReadOperations: " + numOfReadOperations);
    LOG.info("numOfWriteOperations: " + numOfWriteOperations);
    LOG.info("replicationFactor: " + factor);

    OzoneConfiguration configuration = createOzoneConfiguration();
    fileSystem = FileSystem.get(URI.create(prefixPath), configuration);

    contentGenerator = new ContentGenerator(fileSizeInBytes, bufferSize);
    timer = getMetrics().timer("file-create");

    runTests(this::mainMethod);
    return null;
  }

  private void mainMethod(long counter) throws Exception {
    readOperations(counter);
    writeOperations();
  }

  private void readOperations(long counter) throws Exception {

    // Step-1)
    String readPath = prefixPath.concat("/").concat("readPath");
    fileSystem.mkdirs(new Path(readPath));
    createFiles(readPath, fileCountForRead);

    // Step-2)
    int readThreadCount = (readThreadPercentage / 100) * totalThreadCount;

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(factor))
        .setKeyName(generateObjectName(counter))
        .setLocationInfoList(new ArrayList<>())
        .setAcls(OzoneAclUtil.getAclList(ugi.getUserName(), ugi.getGroupNames(),
            ALL, ALL))
        .build();

    ExecutorService readService = Executors.newFixedThreadPool(readThreadCount);
    for (int i = 0; i < numOfReadOperations; i++) {
      readService.execute(new Runnable() {
        @Override
        public void run() {
          try {
            keyManager.listStatus(keyArgs, false, "", fileCountForRead);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
    }
  }

  private void writeOperations() throws IOException {

    // Step-3)
    int writeThreadCount =
        totalThreadCount - ((readThreadPercentage / 100) * totalThreadCount);

    String writePath = prefixPath.concat("/").concat("writePath");
    fileSystem.mkdirs(new Path(writePath));

    ExecutorService writeService =
        Executors.newFixedThreadPool(writeThreadCount);
    for (int i = 0; i < numOfWriteOperations; i++) {
      writeService.execute(new Runnable() {
        @Override
        public void run() {
          try {
            createFiles(writePath, fileCountForWrite);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
    }
  }

  private void createFile(String dir, long counter) throws Exception {
    String fileName = dir.concat("/").concat(RandomStringUtils.
        randomAlphanumeric(length));
    Path file = new Path(fileName);
    if (LOG.isDebugEnabled()) {
      LOG.debug("FilePath:{}", file);
    }
    timer.time(() -> {
      try (FSDataOutputStream output = fileSystem.create(file)) {
        contentGenerator.write(output);
      }
      return null;
    });
  }

  private void createFiles(String dir, int fileCount) throws Exception {
    for (int i = 0; i < fileCount; i++) {
      createFile(dir, i);
    }
  }
}
