package cn.edu.tsinghua.iotdb.kairosdb.dao;

import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.DataPointsParser;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.StringReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteWorker extends Thread {

  private final Gson gson;
  private static final Logger LOGGER = LoggerFactory.getLogger(DataPointsParser.class);
  private String json;

  public WriteWorker(String json) {
    this.json = json;
    GsonBuilder builder = new GsonBuilder();
    gson = builder.disableHtmlEscaping().create();
  }

  @Override
  public void run() {
      try {
        if (json != null && json.length() > 1) {
          StringReader stringReader = new StringReader(json);
          DataPointsParser parser = new DataPointsParser(stringReader, gson);
          parser.parse();
        }
      } catch (Exception e) {
        LOGGER.error("Write worker {} execute parser.parse() failed because ", Thread.currentThread().getName(), e);
      }
  }

}
