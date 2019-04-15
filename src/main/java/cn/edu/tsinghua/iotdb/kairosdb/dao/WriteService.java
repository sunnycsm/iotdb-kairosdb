package cn.edu.tsinghua.iotdb.kairosdb.dao;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteService {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteService.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private long runningTimeMillis = System.currentTimeMillis();
  private boolean stop = false;
  private Statement statement;

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

  public void addDataPoint(String sql) {
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

    @Override
    public void run() {
      while (!stop) {

          try {
            long currentTimeMillis = System.currentTimeMillis();
            long time = currentTimeMillis - runningTimeMillis;
            if (time >= config.SEND_FREQ && statement != null) {
              synchronized (statement) {
                runningTimeMillis = currentTimeMillis;
                statement.executeBatch();
                statement.clearBatch();
                statement.close();
              }
            }
          } catch (Exception e) {
            LOGGER.error("Write batch failed because  ", e);
          }

      }
    }
  }


}
