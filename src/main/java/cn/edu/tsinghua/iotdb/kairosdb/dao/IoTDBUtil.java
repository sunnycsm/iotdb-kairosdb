package cn.edu.tsinghua.iotdb.kairosdb.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBUtil.class);

  private static final String URL = "jdbc:iotdb://%s:%s/";

  private static Connection connection;

  public Statement getStatement() {
    return statement;
  }

  private Statement statement;

  private IoTDBUtil() {
  }

  public static void initConnection(String host, String port, String user, String password)
      throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    connection = DriverManager.getConnection(String.format(URL, host, port), user, password);
  }

  static Connection getConnection() {
    return connection;
  }

  public static void closeConnection() {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());
    }
  }

}
