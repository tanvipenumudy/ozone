/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ozone.recon.schema;

import static org.apache.ozone.recon.schema.SqlDbUtils.TABLE_EXISTS_CHECK;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to create tables that are required for storing directory sizes.
 */
@Singleton
public class DirectorySizeSchemaDefinition implements ReconSchemaDefinition {

  private static final Logger LOG =
      LoggerFactory.getLogger(DirectorySizeSchemaDefinition.class);

  public static final String DIRECTORY_SIZE_TABLE_NAME = "DIRECTORY_SIZE";
  private final DataSource dataSource;
  private DSLContext dslContext;

  @Inject
  DirectorySizeSchemaDefinition(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void initializeSchema() throws SQLException {
    Connection conn = dataSource.getConnection();
    dslContext = DSL.using(conn);
    if (!TABLE_EXISTS_CHECK.test(conn, DIRECTORY_SIZE_TABLE_NAME)) {
      LOG.info("DIRECTORY_SIZE is missing creating new one.");
      createDirectorySizeTable();
    }
  }

  /**
   * Create the directory size table.
   */
  private void createDirectorySizeTable() {
    dslContext.createTableIfNotExists(DIRECTORY_SIZE_TABLE_NAME)
        .column("object_id", SQLDataType.BIGINT.nullable(false))
        .column("total_size", SQLDataType.BIGINT.nullable(false))
        .column("last_updated_timestamp", SQLDataType.TIMESTAMP.defaultValue(DSL.currentTimestamp()))
        .constraint(DSL.constraint("pk_object_id")
            .primaryKey("object_id"))
        .execute();
  }

  public DSLContext getDSLContext() {
    return dslContext;
  }

  public DataSource getDataSource() {
    return dataSource;
  }
} 
