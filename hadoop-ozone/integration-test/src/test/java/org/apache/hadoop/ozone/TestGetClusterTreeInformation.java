/**
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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.net.InnerNode;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NodeImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.proxy.SCMBlockLocationFailoverProxyProvider;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 * This class is to test JMX management interface for scm information.
 */
public class TestGetClusterTreeInformation {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestGetClusterTreeInformation.class);
  private static int numOfDatanodes = 3;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static StorageContainerManager scm;
  private static NodeManager nodeManager;
  private static ClassLoader classLoader =
      Thread.currentThread().getContextClassLoader();

  @BeforeAll
  public static void init() throws IOException, TimeoutException,
      InterruptedException {
    conf = new OzoneConfiguration();
//    conf.set(ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE, "networkTopologyTestFiles/test-topology.xml");
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numOfDatanodes)
        .setNumOfOzoneManagers(3)
        .setNumOfStorageContainerManagers(3)
        .build();
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    nodeManager = scm.getScmNodeManager();
  }

  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testGetClusterTreeInformation() throws IOException {

    SCMBlockLocationFailoverProxyProvider failoverProxyProvider =
        new SCMBlockLocationFailoverProxyProvider(conf);
    failoverProxyProvider.changeCurrentProxy(scm.getSCMNodeId());
    ScmBlockLocationProtocolClientSideTranslatorPB scmBlockLocationClient =
        new ScmBlockLocationProtocolClientSideTranslatorPB(
            failoverProxyProvider);

//    ScmBlockLocationProtocol scmBlockLocationProtocol = TracingUtil
//        .createProxy(scmBlockLocationClient, ScmBlockLocationProtocol.class,
//            conf);

//    List<String> nodes = getNetworkNames();
//    String client = nodes.get(0);
//    List<DatanodeDetails> expectedDatanodeDetails =
//        scm.getBlockProtocolServer().sortDatanodes(nodes, client);
//    List<DatanodeDetails> actualDatanodeDetails =
//        scmBlockLocationClient.sortDatanodes(nodes, client);
//    Assertions.assertEquals(expectedDatanodeDetails, actualDatanodeDetails);

    InnerNode expectedInnerNode = scm.getClusterMap().getClusterTree();
    InnerNode actualInnerNode = scmBlockLocationClient.getClusterTree();
    Assertions.assertEquals(expectedInnerNode, actualInnerNode);

    /*InnerNode expectedInnerNode = scm.getClusterMap().getClusterTree();
    InnerNode actualInnerNode = scmBlockLocationClient.getClusterTree();
    Assertions.assertEquals(expectedInnerNode, actualInnerNode);
    System.out.println("expectedInnerNode: " + expectedInnerNode);
    System.out.println("actualInnerNode: " + actualInnerNode);*/

//    ScmInfo expectedScmInfo = scm.getBlockProtocolServer().getScmInfo();
//    ScmInfo actualScmInfo = scmBlockLocationClient.getScmInfo();
//    Assertions.assertEquals(expectedScmInfo, actualScmInfo);
  }

//  private List<String> getNetworkNames() {
//    return nodeManager.getAllNodes().stream()
//        .map(NodeImpl::getNetworkName)
//        .collect(Collectors.toList());
//  }
}
