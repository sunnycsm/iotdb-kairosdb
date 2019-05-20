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

  public WriteWorker() {
    GsonBuilder builder = new GsonBuilder();
    gson = builder.disableHtmlEscaping().create();
    LOGGER.info("Worker {} has started.", Thread.currentThread().getName());
  }

  @Override
  public void run() {

    while (true) {
      try {
        String json = MessageQueue.getInstance().poll();
        if (json != null && json.length() > 1) {
          StringReader stringReader = new StringReader(json);
          DataPointsParser parser = new DataPointsParser(stringReader, gson);
          parser.parse();
        }
      } catch (Exception e) {
        LOGGER.error("Write worker execute parser.parse() failed because ", e);
      }
    }
  }

}