/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.hdds.scm.net.InnerNode;
import org.apache.hadoop.hdds.scm.net.InnerNodeImpl;
import org.apache.hadoop.hdds.scm.net.NetConstants;
import org.apache.hadoop.hdds.scm.net.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT;

/**
 * The class represents a cluster of computers with a tree hierarchical
 * network topology. In the network topology, leaves represent data nodes
 * (computers) and inner nodes represent datacenter/core-switches/routers that
 * manages traffic in/out of data centers or racks.
 */
public class NetworkTopology {
  public static final Logger LOG =
      LoggerFactory.getLogger(NetworkTopology.class);

  /** The Inner node crate factory. */
  private final InnerNode.Factory factory;
  /** The root cluster tree. */
  private final InnerNode clusterTree;
  /** Depth of all leaf nodes. */
  private final int maxLevel;
  /** Schema manager. */
  private final NodeSchemaManager schemaManager;
  /** Lock to coordinate cluster tree access. */
  private ReadWriteLock netlock = new ReentrantReadWriteLock(true);

  public NetworkTopology(String schemaFile) {
    schemaManager = NodeSchemaManager.getInstance();
    schemaManager.init(schemaFile);
    maxLevel = schemaManager.getMaxLevel();
    factory = InnerNodeImpl.FACTORY;
    clusterTree = factory.newInnerNode(ROOT, null, null,
        NetConstants.ROOT_LEVEL,
        schemaManager.getCost(NetConstants.ROOT_LEVEL));
  }

  @VisibleForTesting
  public NetworkTopology(NodeSchemaManager manager) {
    schemaManager = manager;
    maxLevel = schemaManager.getMaxLevel();
    factory = InnerNodeImpl.FACTORY;
    clusterTree = factory.newInnerNode(ROOT, null, null,
        NetConstants.ROOT_LEVEL,
        schemaManager.getCost(NetConstants.ROOT_LEVEL));
  }

  /** Return the distance cost between two nodes
   * The distance cost from one node to its parent is it's parent's cost
   * The distance cost between two nodes is calculated by summing up their
   * distances cost to their closest common ancestor.
   * @param node1 one node
   * @param node2 another node
   * @return the distance cost between node1 and node2 which is zero if they
   * are the same or {@link Integer#MAX_VALUE} if node1 or node2 do not belong
   * to the cluster
   */

  public int getDistanceCost(Node node1, Node node2) {
    if ((node1 != null && node2 != null && node1.equals(node2)) ||
        (node1 == null && node2 == null))  {
      return 0;
    }
    if (node1 == null || node2 == null) {
      LOG.warn("One of the nodes is a null pointer");
      return Integer.MAX_VALUE;
    }
    int cost = 0;
    netlock.readLock().lock();
    try {
      if ((node1.getAncestor(maxLevel - 1) != clusterTree) ||
          (node2.getAncestor(maxLevel - 1) != clusterTree)) {
        LOG.debug("One of the nodes is outside of network topology");
        return Integer.MAX_VALUE;
      }
      int level1 = node1.getLevel();
      int level2 = node2.getLevel();
      if (level1 > maxLevel || level2 > maxLevel) {
        return Integer.MAX_VALUE;
      }
      while (level1 > level2 && node1 != null) {
        node1 = node1.getParent();
        level1--;
        cost += node1 == null ? 0 : node1.getCost();
      }
      while (level2 > level1 && node2 != null) {
        node2 = node2.getParent();
        level2--;
        cost += node2 == null ? 0 : node2.getCost();
      }
      while (node1 != null && node2 != null && node1 != node2) {
        node1 = node1.getParent();
        node2 = node2.getParent();
        cost += node1 == null ? 0 : node1.getCost();
        cost += node2 == null ? 0 : node2.getCost();
      }
      return cost;
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Sort nodes array by network distance to <i>reader</i> to reduces network
   * traffic and improves performance.
   *
   * As an additional twist, we also randomize the nodes at each network
   * distance. This helps with load balancing when there is data skew.
   *
   * @param reader    Node where need the data
   * @param nodes     Available replicas with the requested data
   * @param activeLen Number of active nodes at the front of the array
   */
  public List<? extends Node> sortByDistanceCost(Node reader,
                                                 List<? extends Node> nodes,
                                                 int activeLen) {
    /** Sort weights for the nodes array */
    if (reader == null) {
      return nodes;
    }
    int[] costs = new int[activeLen];
    for (int i = 0; i < activeLen; i++) {
      costs[i] = getDistanceCost(reader, nodes.get(i));
    }
    // Add cost/node pairs to a TreeMap to sort
    TreeMap<Integer, List<Node>> tree = new TreeMap<Integer, List<Node>>();
    for (int i = 0; i < activeLen; i++) {
      int cost = costs[i];
      Node node = nodes.get(i);
      List<Node> list = tree.get(cost);
      if (list == null) {
        list = Lists.newArrayListWithExpectedSize(1);
        tree.put(cost, list);
      }
      list.add(node);
    }

    List<Node> ret = new ArrayList<>();
    for (List<Node> list: tree.values()) {
      if (list != null) {
        Collections.shuffle(list);
        for (Node n: list) {
          ret.add(n);
        }
      }
    }

    Preconditions.checkState(ret.size() == activeLen,
        "Wrong number of nodes sorted!");
    return ret;
  }
}
