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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.scm.net.NetConstants;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaLoader;
import org.apache.hadoop.hdds.scm.net.NodeSchemaLoader.NodeSchemaLoadResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** The class manages all network topology schemas. */

public final class NodeSchemaManager {
  private static final Logger LOG = LoggerFactory.getLogger(
      NodeSchemaManager.class);

  // All schema saved and sorted from ROOT to LEAF node
  private List<NodeSchema> allSchema;
  // enforcePrefix only applies to INNER_NODE
  private boolean enforcePrefix;
  // max level, includes ROOT level
  private int maxLevel = -1;

  private static volatile NodeSchemaManager instance = null;

  private NodeSchemaManager() {
  }

  public static NodeSchemaManager getInstance() {
    if (instance == null) {
      instance = new NodeSchemaManager();
    }
    return instance;
  }

  public void init(String schemaFile) {
    /**
     * Load schemas from network topology schema configuration file
     */
    NodeSchemaLoadResult result;
    try {
      result = NodeSchemaLoader.getInstance().loadSchemaFromFile(schemaFile);
      allSchema = result.getSchemaList();
      enforcePrefix = result.isEnforePrefix();
      maxLevel = allSchema.size();
    } catch (Throwable e) {
      String msg = "Failed to load schema file:" + schemaFile
          + ", error: " + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    }
  }

  public int getMaxLevel() {
    return maxLevel;
  }

  public int getCost(int level) {
    Preconditions.checkArgument(level <= maxLevel &&
        level >= (NetConstants.ROOT_LEVEL));
    return allSchema.get(level - NetConstants.ROOT_LEVEL).getCost();
  }
}
