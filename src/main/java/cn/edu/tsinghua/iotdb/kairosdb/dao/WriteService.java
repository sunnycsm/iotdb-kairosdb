package cn.edu.tsinghua.iotdb.kairosdb.dao;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteService {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteService.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private long runningTimeMillis = System.currentTimeMillis();
  private boolean stop = false;
  private Statement statement;
  private ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
  // The constants of encoding methods
  private static final String TEXT_ENCODING = "PLAIN";
  private static final String INT64_ENCODING = "TS_2DIFF";
  private static final String DOUBLE_ENCODING = "GORILLA";
  // <path, type>
  private Map<String, String> seriesPaths = new ConcurrentHashMap<>();

  public void stop() {
    stop = true;
    if (statement != null) {
      try {
        statement.executeBatch();
        statement.clearBatch();
      } catch (SQLException e) {
        LOGGER.error("Last batch insert failed because  ", e);
      } finally {
        try {
          statement.close();
        } catch (SQLException e) {
          LOGGER.error("Close statement failed because  ", e);
        }
      }
    }
  }

  private WriteService() {
    try {
      statement = IoTDBUtil.getConnection().createStatement();
    } catch (Exception e) {
      LOGGER.error("create statement failed because ", e);
    }
  }

  public void addSeries(Map<String, String> paths) {
    //rwl.writeLock().lock();
    seriesPaths.putAll(paths);
    //rwl.writeLock().unlock();
  }

  public void addBatch(String sql) {
    rwl.writeLock().lock();
    if (statement == null) {
      try {
        statement = IoTDBUtil.getConnection().createStatement();
        try {
          statement.addBatch(sql);
        } catch (SQLException e) {
          LOGGER.error("Add SQL to batch failed because ", e);
        }
      } catch (Exception e) {
        LOGGER.error("create statement failed because ", e);
      }
    } else {
      try {
        statement.addBatch(sql);
      } catch (SQLException e) {
        LOGGER.error("Add SQL to batch failed because ", e);
      }
    }
    rwl.writeLock().unlock();
  }

  public static WriteService getInstance() {
    return WriteServiceHolder.INSTANCE;
  }

  private static class WriteServiceHolder {

    private static final WriteService INSTANCE = new WriteService();
  }

  public void activate() {
    ExecutorService executorService = Executors.newFixedThreadPool(1);
    executorService.submit(new StatBackLoop());
  }





  class StatBackLoop implements Runnable {

    private String createTimeSeriesSql(String seriesPath, String type) {
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
      return String
          .format("CREATE TIMESERIES %s WITH DATATYPE=%s, ENCODING=%s, COMPRESSOR=SNAPPY", seriesPath,
              datatype, encoding);
    }

    private void createTimeSeries() throws SQLException {
      try (Statement sqlStatement = IoTDBUtil.getConnection().createStatement()) {
        for (Map.Entry<String, String> entry : seriesPaths.entrySet()) {
          sqlStatement.addBatch(createTimeSeriesSql(entry.getKey(), entry.getValue()));
        }
        sqlStatement.executeBatch();
        seriesPaths.clear();
      }
    }

    @Override
    public void run() {
      while (!stop) {

        long currentTimeMillis = System.currentTimeMillis();
        long time = currentTimeMillis - runningTimeMillis;
        if (time >= config.SEND_FREQ && statement != null) {
          runningTimeMillis = currentTimeMillis;
          rwl.readLock().lock();
          try {
            statement.executeBatch();
            statement.clearBatch();
            statement.close();
          } catch (Exception e) {
            // try to create time series
            try {
              createTimeSeries();
              statement.executeBatch();
              statement.clearBatch();
              statement.close();
            } catch (Exception ex) {
              try {
                statement.executeBatch();
                statement.clearBatch();
                statement.close();
              } catch (SQLException exc) {
                LOGGER.error("Very bad exception occur", exc);
              }
            }

          } finally {
            rwl.readLock().unlock();
          }
        }
      }
    }
  }


}
