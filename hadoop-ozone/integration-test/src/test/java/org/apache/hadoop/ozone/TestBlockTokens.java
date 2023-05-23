/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.conf.DefaultConfigManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyManager;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactory;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactoryImpl;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.shell.OzoneShell;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.hdds.DFSConfigKeysLegacy.DFS_DATANODE_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.DFSConfigKeysLegacy.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_EXPIRY_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_ROTATE_CHECK_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_ROTATE_DURATION;
import static org.apache.hadoop.hdds.StringUtils.string2Bytes;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.BLOCK_TOKEN_VERIFICATION_FAILED;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.*;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.apache.ozone.test.GenericTestUtils.assertExceptionContains;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Integration test to verify block tokens in a secure cluster.
 */
@InterfaceAudience.Private
public final class TestBlockTokens {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestBlockTokens.class);
  private static final String TEST_VOLUME = "testvolume";
  private static final String TEST_BUCKET = "testbucket";
  private static final String TEST_FILE = "testfile";
  private static final int ROTATE_DURATION_IN_MS = 3000;
  private static final int EXPIRY_DURATION_IN_MS = 10000;
  private static final int ROTATION_CHECK_DURATION_IN_MS = 100;

  @Rule
  public Timeout timeout = Timeout.seconds(180);

  private static MiniKdc miniKdc;
  private static OzoneAdmin ozoneAdmin;
  private static OzoneConfiguration conf;
  private static File workDir;
  private static File ozoneKeytab;
  private static File spnegoKeytab;
  private static File testUserKeytab;
  private static String testUserPrincipal;
  private static String host;
  private static String clusterId;
  private static String scmId;
  private static int numOfOMs;
  private static String omServiceId;
  private static String omId;
  private static MiniOzoneHAClusterImpl cluster;
  private static OzoneClient client;
  private static OzoneShell ozoneShell;
  private static BlockInputStreamFactory blockInputStreamFactory =
      new BlockInputStreamFactoryImpl();

  @BeforeClass
  public static void init() throws Exception {

    ozoneShell = new OzoneShell();
    conf = new OzoneConfiguration();

    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "localhost");
    //conf.set(OZONE_OM_SERVICE_IDS_KEY, "om-service-test1");

    ExitUtils.disableSystemExit();

    workDir =
        GenericTestUtils.getTestDir(TestBlockTokens.class.getSimpleName());
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    numOfOMs = 3;
    omServiceId = "om-service-test1";
    omId = UUID.randomUUID().toString();;

    startMiniKdc();
    setSecureConfig();
    createCredentialsInKDC();
    setSecretKeysConfig();
    ozoneAdmin = new OzoneAdmin(conf);
    startCluster();
    client = cluster.newClient();
    createTestData();
  }

  private static void createTestData() throws IOException {
    client.getProxy().createVolume(TEST_VOLUME);
    client.getProxy().createBucket(TEST_VOLUME, TEST_BUCKET);
    byte[] data = string2Bytes(RandomStringUtils.randomAlphanumeric(1024));
    OzoneBucket bucket = client.getObjectStore().getVolume(TEST_VOLUME)
        .getBucket(TEST_BUCKET);
    try (OzoneOutputStream out = bucket.createKey(TEST_FILE, data.length)) {
      org.apache.commons.io.IOUtils.write(data, out);
    }
  }

  private void execute(GenericCli shell, String[] args) {
    LOG.info("Executing OzoneShell command with args {}", Arrays.asList(args));
    CommandLine cmd = shell.getCmd();

    CommandLine.IExceptionHandler2<List<Object>> exceptionHandler =
        new CommandLine.IExceptionHandler2<List<Object>>() {
          @Override
          public List<Object> handleParseException(
              CommandLine.ParameterException ex,
              String[] args) {
            throw ex;
          }

          @Override
          public List<Object> handleExecutionException(
              CommandLine.ExecutionException ex,
              CommandLine.ParseResult parseRes) {
            throw ex;
          }
        };

    // Since there is no elegant way to pass Ozone config to the shell,
    // the idea is to use 'set' to place those OM HA configs.
    String[] argsWithHAConf = getHASetConfStrings(args);

    cmd.parseWithHandlers(new CommandLine.RunLast(), exceptionHandler, argsWithHAConf);
  }

  /**
   * Helper function to create a new set of arguments that contains HA configs.
   * @param existingArgs Existing arguments to be fed into OzoneShell command.
   * @return String array.
   */
  private String[] getHASetConfStrings(String[] existingArgs) {
    // Get a String array populated with HA configs first
    String[] res = getHASetConfStrings(existingArgs.length);

    int indexCopyStart = res.length - existingArgs.length;
    // Then copy the existing args to the returned String array
    for (int i = 0; i < existingArgs.length; i++) {
      res[indexCopyStart + i] = existingArgs[i];
    }
    return res;
  }

  private String getSetConfStringFromConf(String key) {
    return String.format("--set=%s=%s", key, cluster.getConf().get(key));
  }

  private String generateSetConfString(String key, String value) {
    return String.format("--set=%s=%s", key, value);
  }

  private String[] getHASetConfStrings(int numOfArgs) {
    assert (numOfArgs >= 0);
    String[] res = new String[1 + 1 + numOfOMs + numOfArgs];
    final int indexOmServiceIds = 0;
    final int indexOmNodes = 1;
    final int indexOmAddressStart = 2;

    res[indexOmServiceIds] = getSetConfStringFromConf(
        OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY);

    String omNodesKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);
    String omNodesVal = cluster.getConf().get(omNodesKey);
    res[indexOmNodes] = generateSetConfString(omNodesKey, omNodesVal);

    String[] omNodesArr = omNodesVal.split(",");
    // Sanity check
    System.out.println(numOfOMs);
    System.out.println(omNodesArr.length);
    assert (omNodesArr.length == numOfOMs);
    for (int i = 0; i < numOfOMs; i++) {
      res[indexOmAddressStart + i] =
          getSetConfStringFromConf(ConfUtils.addKeySuffixes(
              OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId, omNodesArr[i]));
    }

    return res;
  }

  @AfterClass
  public static void stop() {
    miniKdc.stop();
    IOUtils.close(LOG, client);
    if (cluster != null) {
      cluster.stop();
    }
    DefaultConfigManager.clearDefaultConfigs();
  }

  @Test
  public void blockTokensHappyCase() throws Exception {
    ManagedSecretKey currentScmKey =
        getScmSecretKeyManager().getCurrentSecretKey();
    OmKeyInfo keyInfo = getTestKeyInfo();

    // assert block token points to the current SCM key.
    assertEquals(currentScmKey.getId(), extractSecretKeyId(keyInfo));

    // and the keyInfo can be used to read from datanodes.
    readDataWithoutRetry(keyInfo);

    // after the rotation passes, the old token is still usable.
    waitFor(
        () -> !Objects.equals(getScmSecretKeyManager().getCurrentSecretKey(),
            currentScmKey),
        ROTATION_CHECK_DURATION_IN_MS,
        ROTATE_DURATION_IN_MS + ROTATION_CHECK_DURATION_IN_MS);
    readDataWithoutRetry(keyInfo);
  }

  @Test
  public void blockTokenFailsOnExpiredSecretKey() throws Exception {
    OmKeyInfo keyInfo = getTestKeyInfo();
    UUID secretKeyId = extractSecretKeyId(keyInfo);
    readDataWithoutRetry(keyInfo);

    // wait until the secret key expires.
    ManagedSecretKey secretKey =
        requireNonNull(getScmSecretKeyManager().getSecretKey(secretKeyId));
    waitFor(secretKey::isExpired, ROTATION_CHECK_DURATION_IN_MS,
        EXPIRY_DURATION_IN_MS);
    assertTrue(secretKey.isExpired());
    // verify that the read is denied because of the expired secret key.
    StorageContainerException ex = assertThrows(StorageContainerException.class,
        () -> readDataWithoutRetry(keyInfo));
    assertEquals(BLOCK_TOKEN_VERIFICATION_FAILED, ex.getResult());
    assertExceptionContains(
        "Token can't be verified due to expired secret key", ex);
  }

  @Test
  public void blockTokenOnExpiredSecretKeyRetrySuccessful() throws Exception {
    OmKeyInfo keyInfo = getTestKeyInfo();
    UUID secretKeyId = extractSecretKeyId(keyInfo);
    readDataWithoutRetry(keyInfo);

    // wait until the secret key expires.
    ManagedSecretKey secretKey =
        requireNonNull(getScmSecretKeyManager().getSecretKey(secretKeyId));
    waitFor(secretKey::isExpired, ROTATION_CHECK_DURATION_IN_MS,
        EXPIRY_DURATION_IN_MS);
    assertTrue(secretKey.isExpired());
    // verify that the read is denied because of the expired secret key.
    readData(keyInfo, k -> {
      try {
        return getTestKeyInfo();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void blockTokenFailsOnWrongSecretKeyId() throws Exception {
    OmKeyInfo keyInfo = getTestKeyInfo();
    // replace block token secret key id with wrong id.
    for (OmKeyLocationInfoGroup v : keyInfo.getKeyLocationVersions()) {
      for (OmKeyLocationInfo l : v.getLocationList()) {
        Token<OzoneBlockTokenIdentifier> token = l.getToken();
        OzoneBlockTokenIdentifier tokenId = token.decodeIdentifier();
        tokenId.setSecretKeyId(UUID.randomUUID());
        token.setID(tokenId.getBytes());
      }
    }

    // verify that the read is denied because of the unknown secret key.
    StorageContainerException ex =
        assertThrows(StorageContainerException.class,
            () -> readDataWithoutRetry(keyInfo));
    assertEquals(BLOCK_TOKEN_VERIFICATION_FAILED, ex.getResult());
    assertExceptionContains("Can't find the signing secret key", ex);
  }

  @Test
  public void blockTokenFailsOnWrongPassword() throws Exception {
    OmKeyInfo keyInfo = getTestKeyInfo();
    // replace block token secret key id with wrong id.
    for (OmKeyLocationInfoGroup v : keyInfo.getKeyLocationVersions()) {
      for (OmKeyLocationInfo l : v.getLocationList()) {
        Token<OzoneBlockTokenIdentifier> token = l.getToken();
        token.setPassword(RandomUtils.nextBytes(100));
      }
    }

    // verify that the read is denied because of the unknown secret key.
    StorageContainerException ex =
        assertThrows(StorageContainerException.class,
            () -> readDataWithoutRetry(keyInfo));
    assertEquals(BLOCK_TOKEN_VERIFICATION_FAILED, ex.getResult());
    assertExceptionContains("Invalid token for user", ex);
  }

  @Test
  public void testGetCurrentSecretKey() {
    String[] args = new String[] {"om", "fetch-current-key", "--service-id=om-service-test1"};
    execute(ozoneAdmin, args);
  }

  private UUID extractSecretKeyId(OmKeyInfo keyInfo) throws IOException {
    OmKeyLocationInfo locationInfo =
        keyInfo.getKeyLocationVersions().get(0).getLocationList().get(0);
    Token<OzoneBlockTokenIdentifier> token = locationInfo.getToken();
    return token.decodeIdentifier().getSecretKeyId();
  }

  private OmKeyInfo getTestKeyInfo() throws IOException {
    OmKeyArgs arg = new OmKeyArgs.Builder()
        .setVolumeName(TEST_VOLUME)
        .setBucketName(TEST_BUCKET)
        .setKeyName(TEST_FILE)
        .build();
    return cluster.getOzoneManager()
        .getKeyInfo(arg, false).getKeyInfo();
  }

  private void readDataWithoutRetry(OmKeyInfo keyInfo) throws IOException {
    readData(keyInfo, null);
  }

  private void readData(OmKeyInfo keyInfo,
      Function<OmKeyInfo, OmKeyInfo> retryFunc) throws IOException {
    XceiverClientFactory xceiverClientManager =
        ((RpcClient) client.getProxy()).getXceiverClientManager();
    try (InputStream is = KeyInputStream.getFromOmKeyInfo(keyInfo,
        xceiverClientManager,
        false, retryFunc, blockInputStreamFactory)) {
      byte[] buf = new byte[100];
      int readBytes = is.read(buf, 0, 100);
      assertEquals(100, readBytes);
    }
  }

  private SecretKeyManager getScmSecretKeyManager() {
    return cluster.getActiveSCM().getSecretKeyManager();
  }

  private static void setSecretKeysConfig() {
    // Secret key lifecycle configs.
    conf.set(HDDS_SECRET_KEY_ROTATE_CHECK_DURATION,
        ROTATION_CHECK_DURATION_IN_MS + "ms");
    conf.set(HDDS_SECRET_KEY_ROTATE_DURATION, ROTATE_DURATION_IN_MS + "ms");
    conf.set(HDDS_SECRET_KEY_EXPIRY_DURATION, EXPIRY_DURATION_IN_MS + "ms");

    // enable tokens
    conf.setBoolean(HDDS_BLOCK_TOKEN_ENABLED, true);
    conf.setBoolean(HDDS_CONTAINER_TOKEN_ENABLED, true);
  }

  private static void createCredentialsInKDC() throws Exception {
    ScmConfig scmConfig = conf.getObject(ScmConfig.class);
    SCMHTTPServerConfig httpServerConfig =
        conf.getObject(SCMHTTPServerConfig.class);
    createPrincipal(ozoneKeytab, scmConfig.getKerberosPrincipal());
    createPrincipal(spnegoKeytab, httpServerConfig.getKerberosPrincipal());
    createPrincipal(testUserKeytab, testUserPrincipal);
  }

  private static void createPrincipal(File keytab, String... principal)
      throws Exception {
    miniKdc.createPrincipal(keytab, principal);
  }

  private static void startMiniKdc() throws Exception {
    Properties securityProperties = MiniKdc.createConf();
    miniKdc = new MiniKdc(securityProperties, workDir);
    miniKdc.start();
  }

  private static void setSecureConfig() throws IOException {
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    host = InetAddress.getLocalHost().getCanonicalHostName()
        .toLowerCase();

    conf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS.name());

    String curUser = UserGroupInformation.getCurrentUser().getUserName();
    conf.set(OZONE_ADMINISTRATORS, curUser);

    String realm = miniKdc.getRealm();
    String hostAndRealm = host + "@" + realm;
    conf.set(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, "scm/" + hostAndRealm);
    conf.set(HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY, "HTTP_SCM/" + hostAndRealm);
    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY, "scm/" + hostAndRealm);
    conf.set(OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY, "HTTP_OM/" + hostAndRealm);
    conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, "scm/" + hostAndRealm);

    ozoneKeytab = new File(workDir, "scm.keytab");
    spnegoKeytab = new File(workDir, "http.keytab");
    testUserKeytab = new File(workDir, "testuser.keytab");
    testUserPrincipal = "test@" + realm;

    conf.set(HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY,
        ozoneKeytab.getAbsolutePath());
    conf.set(HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY,
        spnegoKeytab.getAbsolutePath());
    conf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY,
        ozoneKeytab.getAbsolutePath());
    conf.set(OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE,
        spnegoKeytab.getAbsolutePath());
    conf.set(DFS_DATANODE_KERBEROS_KEYTAB_FILE_KEY,
        ozoneKeytab.getAbsolutePath());
  }

  private static void startCluster()
      throws IOException, TimeoutException, InterruptedException {
    OzoneManager.setTestSecureOmFlag(true);
    MiniOzoneCluster.Builder builder = MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setSCMServiceId("TestSecretKey")
        .setOMServiceId("om-service-test1")
        .setScmId(scmId)
        .setOmId(omId)
        .setNumDatanodes(5)
        .setNumOfStorageContainerManagers(3)
        .setNumOfOzoneManagers(numOfOMs);

    cluster = (MiniOzoneHAClusterImpl) builder.build();
    cluster.waitForClusterToBeReady();
  }

}
