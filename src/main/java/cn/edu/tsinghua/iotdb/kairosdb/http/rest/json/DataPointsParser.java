package cn.edu.tsinghua.iotdb.kairosdb.http.rest.json;

import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.util.Util;
import cn.edu.tsinghua.iotdb.kairosdb.util.ValidationException;
import cn.edu.tsinghua.iotdb.kairosdb.util.Validator;
import com.google.common.collect.ImmutableSortedMap;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.util.Map;


public class DataPointsParser {

  private final Reader inputStream;
  private final Gson gson;

  private int ingestTime;
  private int dataPointCount;

  public DataPointsParser(Reader stream, Gson gson) {
    this.inputStream = stream;
    this.gson = gson;
  }

  public int getIngestTime() {
    return ingestTime;
  }

  public int getDataPointCount() {
    return dataPointCount;
  }

  public ValidationErrors parse() throws IOException {

    long start = System.currentTimeMillis();
    ValidationErrors validationErrors = new ValidationErrors();

    try (JsonReader reader = new JsonReader(inputStream)) {
      int metricCount = 0;

      if (reader.peek().equals(JsonToken.BEGIN_ARRAY)) {
        try {
          reader.beginArray();

          while (reader.hasNext()) {
            NewMetric metric = parseMetric(reader);
            validateAndAddDataPoints(metric, validationErrors);
            metricCount++;
          }
        } catch (EOFException e) {
          validationErrors.addErrorMessage("Invalid json. No content due to end of input.");
        }

        reader.endArray();
      } else if (reader.peek().equals(JsonToken.BEGIN_OBJECT)) {
        NewMetric metric = parseMetric(reader);
        validateAndAddDataPoints(metric, validationErrors);
      } else {
        validationErrors.addErrorMessage("Invalid start of json.");
      }

    } catch (EOFException e) {
      validationErrors.addErrorMessage("Invalid json. No content due to end of input.");
    }

    ingestTime = (int) (System.currentTimeMillis() - start);

    return validationErrors;
  }

  private NewMetric parseMetric(JsonReader reader) {
    NewMetric metric;
    try {
      metric = gson.fromJson(reader, NewMetric.class);
    } catch (IllegalArgumentException e) {
      // Happens when parsing data points where one of the pair is missing (timestamp or value)
      throw new JsonSyntaxException("Invalid JSON");
    }
    return metric;
  }

  private boolean validateAndAddDataPoints(NewMetric metric, ValidationErrors errors) {
    ValidationErrors validationErrors = new ValidationErrors();


    if (!validationErrors.hasErrors()) {
      //ImmutableSortedMap<String, String> tags = ImmutableSortedMap.copyOf(metric.getTags());

      if (metric.getTimestamp() != null && metric.getValue() != null) {
        String type = null;
        try {
          type = findType(metric.getValue());
        } catch (ValidationException e) {
          validationErrors.addErrorMessage(e.getMessage());
        }

        try {
          ValidationErrors tErrors = MetricsManager.addDatapoint(metric.getName(), metric.getTags(), type, metric.getTimestamp(),
              metric.getValue().getAsString());
          if (null != tErrors) {
            validationErrors.add(tErrors);
          }
        } catch (SQLException e) {
          validationErrors.addErrorMessage(e.getMessage());
        }
      }

      if (metric.getDatapoints() != null && metric.getDatapoints().length > 0) {
        for (JsonElement[] dataPoint : metric.getDatapoints()) {
          Long timestamp = null;
          if (!dataPoint[0].isJsonNull()) {
            timestamp = dataPoint[0].getAsLong();
          }

          String type = null;
          if (dataPoint.length > 2) {
            type = dataPoint[2].getAsString();
          }

          if (type == null) {
            try {
              type = findType(dataPoint[1]);
            } catch (ValidationException e) {
              validationErrors.addErrorMessage(e.getMessage());
              continue;
            }
          }

          try {
            ValidationErrors tErrors = MetricsManager
                .addDatapoint(metric.getName(), metric.getTags(), type, timestamp,
                    dataPoint[1].getAsString());
            if (null != tErrors) {
              validationErrors.add(tErrors);
            }
          } catch (SQLException e) {
            validationErrors.addErrorMessage(e.getMessage());
          }
          dataPointCount++;
        }
      }
    }

    errors.add(validationErrors);

    return !validationErrors.hasErrors();
  }

  private String findType(JsonElement value) throws ValidationException {
    if (!value.isJsonPrimitive()) {
      throw new ValidationException("value is an invalid type");
    }

    JsonPrimitive primitiveValue = (JsonPrimitive) value;
    if (primitiveValue.isNumber() || (primitiveValue.isString() && Util
        .isNumber(value.getAsString()))) {
      String v = value.getAsString();

      if (!v.contains(".")) {
        return "long";
      } else {
        return "double";
      }
    } else {
      return "string";
    }
  }

  private static class NewMetric {

    private String name;
    private Long timestamp = null;
    private Long time = null;
    private JsonElement value;
    private Map<String, String> tags;
    private JsonElement[][] datapoints;

    public String getName() {
      return name;
    }

    public Long getTimestamp() {
      if (time != null) {
        return time;
      } else {
        return timestamp;
      }
    }

    public JsonElement getValue() {
      return value;
    }

    public Map<String, String> getTags() {
      return tags;
    }

    public JsonElement[][] getDatapoints() {
      return datapoints;
    }

  }

}
