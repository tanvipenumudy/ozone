package org.apache.hadoop.ozone.debug.ldb;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.ozone.debug.DBDefinitionFactory;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo; // Assuming keyTable stores OmKeyInfo
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo; // Assuming fileTable stores OmDirectoryInfo or OmKeyInfo (files are keys)
// openFileTable likely stores OmKeyInfo as well.
// Let's assume a common superclass or direct field access.
// For OmTable, value types could be OmBucketInfo, OmKeyInfo, OmVolumeInfo, etc.
// We'll need to use reflection if there's no common interface/superclass with getDataSize()

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Tool to iterate over specified OM tables (fileTable, keyTable, openFileTable, openKeyTable),
 * sum up the 'dataSize' field from their values, and output the sums in JSON format.
 */
@CommandLine.Command(
    name = "summarize-datasize",
    description = "Sums up the 'dataSize' field for fileTable, keyTable, openFileTable, and openKeyTable."
)
public class DataSizeSummarizer extends AbstractSubcommand implements Callable<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(DataSizeSummarizer.class);

  @CommandLine.ParentCommand
  private RDBParser parent; // This will provide the --dbPath

  // Define the tables we are interested in.
  // NOTE: These are logical names. The actual ColumnFamily names might be different
  // in the DBDefinition. We'll look them up by these common names.
  // OM DB typically has: "keyTable", "fileTable", "openKeyTable", "directoryTable"
  // Let's assume "fileTable" refers to "keyTable" where keys are files, or "directoryTable" for pure directories.
  // For simplicity, I'll use the names as provided in the request, assuming they map correctly.
  private static final String[] TARGET_TABLE_NAMES =
      {"keyTable", "fileTable", "openKeyTable", "openFileTable"}; // Adjust if actual table names in DBDefinition differ

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .enable(SerializationFeature.INDENT_OUTPUT); // For pretty printing JSON

  @Override
  public Void call() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration(); // Or get from parent if it has one

    // Ensure DBDefinitionFactory is initialized (if it has static state for schema versions, though less relevant for OM)
    DBDefinitionFactory.setDnDBSchemaVersion("V3"); // Default, though OM doesn't use this

    String dbPath = parent.getDbPath();
    if (dbPath == null || dbPath.isEmpty()) {
      err().println("Error: DB Path must be specified using --db-path option on the parent command.");
      return null;
    }
    dbPath = removeTrailingSlashIfNeeded(dbPath);

    DBDefinition dbDefinition = DBDefinitionFactory.getDefinition(Paths.get(dbPath), conf);
    if (dbDefinition == null) {
      err().println("Error: Could not determine DB definition for path: " + dbPath +
          ". Ensure it's a valid OM, SCM, or Datanode DB path.");
      return null;
    }

    List<ColumnFamilyDescriptor> cfDescList = RocksDBUtils.getColumnFamilyDescriptors(dbPath);
    final List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
    Map<String, Long> tableDataSizes = new HashMap<>();

    try (ManagedRocksDB db = ManagedRocksDB.openReadOnly(dbPath, cfDescList, cfHandleList)) {
      for (String tableName : TARGET_TABLE_NAMES) {
        long totalDataSizeForTable = 0;

        DBColumnFamilyDefinition<?, ?> columnFamilyDefinition = dbDefinition.getColumnFamily(tableName);
        if (columnFamilyDefinition == null) {
          LOG.warn("Table with name '{}' not found in DB definition for {}. Skipping.", tableName, dbPath);
          tableDataSizes.put(tableName, 0L); // Record as 0 if not found
          continue;
        }

        ColumnFamilyHandle cfHandle = getColumnFamilyHandle(
            columnFamilyDefinition.getName().getBytes(UTF_8), cfHandleList);
        if (cfHandle == null) {
          LOG.error("Could not get ColumnFamilyHandle for table '{}'. Skipping.", tableName);
          tableDataSizes.put(tableName, 0L);
          continue;
        }

        LOG.info("Processing table: {}", tableName);
        ManagedRocksIterator iterator = null;
        try {
          iterator = new ManagedRocksIterator(db.get().newIterator(cfHandle));
          iterator.get().seekToFirst();

          while (iterator.get().isValid()) {
            byte[] valueBytes = iterator.get().value();
            if (valueBytes != null) {
              try {
                Object valueObject = columnFamilyDefinition.getValueCodec().fromPersistedFormat(valueBytes);
                totalDataSizeForTable += getDataSizeFromObject(valueObject, tableName);
              } catch (IOException e) {
                LOG.error("Error deserializing value for table {}: {}", tableName, e.getMessage());
                // Potentially skip this entry or handle error
              }
            }
            iterator.get().next();
          }
        } finally {
          IOUtils.closeQuietly(iterator);
        }
        tableDataSizes.put(tableName, totalDataSizeForTable);
        LOG.info("Total dataSize for table {}: {}", tableName, totalDataSizeForTable);
      }

      // Output the results as JSON to stdout
      ObjectWriter writer = OBJECT_MAPPER.writerWithDefaultPrettyPrinter();
      out().println(writer.writeValueAsString(tableDataSizes));

    } catch (RocksDBException | IOException e) {
      LOG.error("Error accessing RocksDB: {}", e.getMessage(), e);
      err().println("Error accessing RocksDB: " + e.getMessage());
      throw e; // Re-throw to indicate failure
    } finally {
      for (final ColumnFamilyHandle cfHandle : cfHandleList) {
        cfHandle.close();
      }
    }
    return null;
  }

  /**
   * Attempts to get the 'dataSize' field from an object using reflection.
   * This is a generic approach. Specific handling for known types would be more robust.
   */
  private long getDataSizeFromObject(Object valueObject, String tableNameForContext) {
    if (valueObject == null) {
      return 0;
    }

    // Example of type-specific handling (more robust)
    // if (valueObject instanceof OmKeyInfo) {
    //   return ((OmKeyInfo) valueObject).getDataSize();
    // } else if (valueObject instanceof OmDirectoryInfo) {
    //    return 0; // Directories in OM typically don't have a direct dataSize, their content does.
    // }
    // ... add other known types

    // Generic reflection-based approach
    try {
      Field dataSizeField = null;
      try {
        // Try to get a public field or one from a superclass
        dataSizeField = valueObject.getClass().getField("dataSize");
      } catch (NoSuchFieldException e) {
        // Try declared field (private, protected, package-private)
        dataSizeField = getDeclaredFieldRecursive(valueObject.getClass(), "dataSize");
      }

      if (dataSizeField != null) {
        dataSizeField.setAccessible(true); // Allow access to private fields
        Object dataSizeValue = dataSizeField.get(valueObject);
        if (dataSizeValue instanceof Number) {
          return ((Number) dataSizeValue).longValue();
        } else {
          LOG.warn("Field 'dataSize' in table '{}' for object {} is not a Number: type is {}",
              tableNameForContext, valueObject.getClass().getSimpleName(), dataSizeValue.getClass().getSimpleName());
        }
      } else {
        LOG.warn("Field 'dataSize' not found in table '{}' for object {}",
            tableNameForContext, valueObject.getClass().getName());
      }
    } catch (IllegalAccessException e) {
      LOG.error("Cannot access 'dataSize' field in table '{}' for object {}: {}",
          tableNameForContext, valueObject.getClass().getName(), e.getMessage());
    } catch (SecurityException | NoSuchFieldException e) {
      LOG.error("Security exception accessing 'dataSize' field in table '{}' for object {}: {}",
          tableNameForContext, valueObject.getClass().getName(), e.getMessage());
    }
    return 0; // Return 0 if field not found, not accessible, or not a number
  }

  /**
   * Recursively searches for a declared field in the class and its superclasses.
   */
  private Field getDeclaredFieldRecursive(Class<?> clazz, String fieldName) throws NoSuchFieldException {
    try {
      return clazz.getDeclaredField(fieldName);
    } catch (NoSuchFieldException e) {
      if (clazz.getSuperclass() != null) {
        return getDeclaredFieldRecursive(clazz.getSuperclass(), fieldName);
      }
      throw e;
    }
  }


  private ColumnFamilyHandle getColumnFamilyHandle(
      byte[] name, List<ColumnFamilyHandle> columnFamilyHandles) {
    return columnFamilyHandles
        .stream()
        .filter(
            handle -> {
              try {
                return Arrays.equals(handle.getName(), name);
              } catch (Exception ex) {
                // RocksDBException from handle.getName() should be caught
                LOG.error("Error getting name from ColumnFamilyHandle", ex);
                throw new RuntimeException(ex);
              }
            })
        .findAny()
        .orElse(null);
  }

  private String removeTrailingSlashIfNeeded(String dbPath) {
    if (dbPath.endsWith("/")) { // OzoneConsts.OZONE_URI_DELIMITER
      dbPath = dbPath.substring(0, dbPath.length() - 1);
    }
    return dbPath;
  }
}
