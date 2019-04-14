package cn.edu.tsinghua.iotdb.kairosdb.dao;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.ValidationErrors;
import com.google.common.collect.ImmutableSortedMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsManager.class);

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String ERROR_OUTPUT_FORMATTER = "%s: %s";

  // The metadata maintained in the memory
  private static final HashMap<String, Map<String, Integer>> tagOrder = new HashMap<>();

  // The SQL will be used to create metadata
  private static final String SYSTEM_CREATE_SQL = "CREATE TIMESERIES root.SYSTEM.TAG_NAME_INFO.%s WITH DATATYPE=%s, ENCODING=%s";

  // The constants of encoding methods
  private static final String TEXT_ENCODING = "PLAIN";
  private static final String INT64_ENCODING = "TS_2DIFF";
  private static final String INT32_ENCODING = "TS_2DIFF";
  private static final String DOUBLE_ENCODING = "GORILLA";

  // Storage group relevant config
  private static int storageGroupSize = config.STORAGE_GROUP_SIZE;
  private static final String STORAGE_GROUP_PREFIX = "group_";
  public static double totalInsertTime = 0;

  private MetricsManager() {
  }

  /**
   * Load all of the metadata from database into memory.
   * If the storage groups of metadata exist, load out the content.
   * If the storage groups of metadata don't exist, create all of the TIMESERIES for persistent.
   */
  public static void loadMetadata() {
    LOGGER.info("Start loading system data.");
    Statement statement = null;
    try {
      // Judge whether the TIMESERIES(root.SYSTEM.TAG_NAME_INFO) has been created
      statement = IoTDBUtil.getConnection().createStatement();
      statement.execute(String.format("SHOW TIMESERIES root.%s", "SYSTEM"));
      ResultSet rs = statement.getResultSet();
      if (rs.next()) {
        /* Since the TIMESERIES are created
         * Recover the tag_key-potion mapping */
        statement = IoTDBUtil.getConnection().createStatement();
        statement.execute(String.format("SELECT metric_name,tag_name,tag_order FROM %s", "root.SYSTEM.TAG_NAME_INFO"));
        rs = statement.getResultSet();
        while (rs.next()) {
          String name = rs.getString(2);
          String tagName = rs.getString(3);
          Integer pos = rs.getInt(4);
          tagOrder.computeIfAbsent(name, k -> new HashMap<>());
          Map<String, Integer> temp = tagOrder.get(name);
          temp.put(tagName, pos);
        }

        // Read the size of storage group
        statement.execute(String.format("SELECT storage_group_size FROM %s",
            "root.SYSTEM.TAG_NAME_INFO"));
        rs = statement.getResultSet();
        if (rs.next()) {
          storageGroupSize = rs.getInt(2);
        } else {
          LOGGER.error("Database metadata has broken, please reload a new database.");
          System.exit(1);
        }
      } else {
        /* Since the TIMESERIES are not created
         * Create all the relevant TIMESERIES of metadata */
        statement.execute(String.format("SET STORAGE GROUP TO root.%s", "SYSTEM"));
        statement.execute(String.format(SYSTEM_CREATE_SQL, "metric_name", "TEXT", TEXT_ENCODING));
        statement.execute(String.format(SYSTEM_CREATE_SQL, "tag_name", "TEXT", TEXT_ENCODING));
        statement.execute(String.format(SYSTEM_CREATE_SQL, "tag_order", "INT32", INT32_ENCODING));

        // Initialize the storage group with STORAGE_GROUP_SIZE which is specified by config.properties
        statement.execute(String.format(SYSTEM_CREATE_SQL, "storage_group_size", "INT32", INT32_ENCODING));
        statement.execute(String.format(
            "insert into root.SYSTEM.TAG_NAME_INFO(timestamp, storage_group_size) values(%s, %s);",
            new Date().getTime(), storageGroupSize));
        for (int i = 0; i < storageGroupSize; i++) {
          statement.execute(String.format("SET STORAGE GROUP TO root.%s%s", STORAGE_GROUP_PREFIX, i));
        }
      }

    } catch (SQLException e) {
      LOGGER.error(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
    } finally {
      close(statement);
    }
    LOGGER.info("Finish loading system data.");
  }

  /**
   * Create a new TIMESERIES with given name, path and type.
   *
   * @param metricName The name of metric will be placed at the end of the path
   * @param path The path prefix of TIMESERIES
   * @param type The type of incoming data
   * @throws SQLException The exception will be throw when some errors occur while creating
   */
  private static void createNewMetric(String metricName, String path, String type) throws SQLException {
    String datatype;
    String encoding;
    switch (type) {
      case "long":
        datatype = "INT64";
        encoding = INT64_ENCODING;
        break;
      case "double":
        datatype = "DOUBLE";
        encoding = DOUBLE_ENCODING;
        break;
      default:
        datatype = "TEXT";
        encoding = TEXT_ENCODING;
    }
    try (Statement statement = IoTDBUtil.getConnection().createStatement()) {
      statement.execute(String
          .format("CREATE TIMESERIES root.%s%s.%s WITH DATATYPE=%s, ENCODING=%s, COMPRESSOR=SNAPPY",
              getStorageGroupName(metricName), path, metricName, datatype, encoding));
    }
  }

  /**
   * Add a new datapoint to database,
   * and automatically create corresponding TIMESERIES to store it.
   *
   * @param name The name of the metric
   * @param tags The tags of the datapoint(at least one)
   * @param type The type of the datapoint value(int, double, text)
   * @param timestamp The timestamp of the datapoint
   * @param value The value of the datapoint
   * @return Null if the datapoint has been correctly insert, otherwise, the errors in ValidationErrors
   * @throws SQLException The SQLException will be thrown when unexpected error occurs
   */
  public static ValidationErrors addDatapoint(String name, ImmutableSortedMap<String, String> tags,
      String type, Long timestamp, String value) throws SQLException {
    ValidationErrors validationErrors = new ValidationErrors();
    if (null == tags) {
      LOGGER.error("metric {} have no tag", name);
      validationErrors.addErrorMessage(String.format("metric %s have no tag", name));
      return validationErrors;
    }

    long st = System.nanoTime();
    HashMap<Integer, String> orderTagKeyMap = getMapping(name, tags);
    long elapse = System.nanoTime() - st;
    LOGGER.info("[HashMap<Integer, String> orderTagKeyMap = getMapping(name, tags)] execution time: {} ms",
        String.format("%.2f", elapse / 1000000.0));

    Map<String, Integer> metricTags = tagOrder.get(name);

    if (type.equals("string")) {
      value = String.format("\"%s\"", value);
    }

    // Generate the path
    StringBuilder pathBuilder = new StringBuilder();
    int i = 0;
    int counter = 0;
    while (i < metricTags.size() && counter < tags.size()) {
      String path = tags.get(orderTagKeyMap.get(i));
      pathBuilder.append(".");
      if (null == path) {
        pathBuilder.append("d");
      } else {
        pathBuilder.append(path);
        counter++;
      }
      i++;
    }

    String insertingSql = String
        .format("insert into root.%s%s(timestamp,%s) values(%s,%s);", getStorageGroupName(name),
            pathBuilder.toString(), name, timestamp, value);
    st = System.nanoTime();
    PreparedStatement pst = null;
    try {

      pst = IoTDBUtil.getPreparedStatement(insertingSql, null);
      pst.executeUpdate();
    } catch (IoTDBSQLException e) {
      try {
        createNewMetric(name, pathBuilder.toString(), type);
        LOGGER.info("TIMESERIES(root{}.{}) has been created.", pathBuilder, name);
        pst = IoTDBUtil.getPreparedStatement(insertingSql, null);
        pst.executeUpdate();
      } catch (IoTDBSQLException e1) {
        validationErrors.addErrorMessage(
            String.format(ERROR_OUTPUT_FORMATTER, e1.getClass().getName(), e1.getMessage()));
        return validationErrors;
      }
    } catch (SQLException e) {
      validationErrors.addErrorMessage(
          String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
      return validationErrors;
    } finally {
      close(pst);
    }
    elapse = System.nanoTime() - st;
    totalInsertTime += elapse;
    System.out.print("[pst.executeUpdate()] execution time: ");
    System.out.println(String.format("%.4f", elapse / 1000000.0) + " ms");
    System.out.print("totalInsertTime : ");
    System.out.println(String.format("%.4f", totalInsertTime / 1000000.0) + " ms");
    return null;
  }

  /**
   * Get or generate the mapping rule from position to tag_key of the given metric name and tags.
   *
   * @param name The metric name will be mapping
   * @param tags The tags will be computed
   * @return The mapping rule from position to tag_key
   */
  private static HashMap<Integer, String> getMapping(String name, Map<String, String> tags) {
    Map<String, Integer> tagKeyOrderMap = tagOrder.get(name);
    HashMap<Integer, String> mapping = new HashMap<>();
    HashMap<String, Integer> cache = new HashMap<>();
    if (null == tagKeyOrderMap) {
      // The metric name appears for the first time
      tagKeyOrderMap = new HashMap<>();
      Integer order = 0;
      for (String tagKey : tags.keySet()) {
        tagKeyOrderMap.put(tagKey, order);
        mapping.put(order, tagKey);
        cache.put(tagKey, order);
        order++;
      }
      tagOrder.put(name, tagKeyOrderMap);
      persistMappingCache(name, cache);
    } else {
      // The metric name exists
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        Integer pos = tagKeyOrderMap.get(tag.getKey());
        if (null == pos) {
          pos = tagKeyOrderMap.size();
          tagKeyOrderMap.put(tag.getKey(), pos);
          cache.put(tag.getKey(), pos);
          persistMappingCache(name, cache);
        }
        mapping.put(pos, tag.getKey());
      }
    }
    return mapping;
  }

  /**
   * Persist the new mapping rule into database.
   *
   * @param metricName The name of the specific metric
   * @param cache The mapping cache will be persisted into database
   */
  private static void persistMappingCache(String metricName, Map<String, Integer> cache) {
    for (Map.Entry<String, Integer> entry : cache.entrySet()) {
      long timestamp = new Date().getTime();
      String sql = String.format(
          "insert into root.SYSTEM.TAG_NAME_INFO(timestamp, metric_name, tag_name, tag_order) values(%s, \"%s\", \"%s\", %s);",
          timestamp, metricName, entry.getKey(), entry.getValue());
      try (Statement statement = IoTDBUtil.getConnection().createStatement()) {
        statement.execute(sql);
      } catch (SQLException e) {
        LOGGER.error(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
      }
    }
  }

  /**
   * Generate a corresponding storage group name of the given metric.
   *
   * @param metricName The name of the specific metric
   * @return The corresponding storage group name of the given metric
   */
  private static String getStorageGroupName(String metricName) {
    if (metricName == null) {
      LOGGER.error("MetricsManager.getStorageGroupName(String metricName): metricName could not be null.");
      return "null";
    }
    int hashCode = metricName.hashCode();
    return String.format("%s%s", STORAGE_GROUP_PREFIX, Math.abs(hashCode) % storageGroupSize);
  }

  /**
   * Close the statement no matter whether it is open and ignore any exception.
   *
   * @param statement The statement will be closed
   */
  private static void close(Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        LOGGER.warn(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
      }
    }
  }

}
