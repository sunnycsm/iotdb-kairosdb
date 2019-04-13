package cn.edu.tsinghua.iotdb.kairosdb.query.group_by;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.lang.reflect.Type;

public class GroupByDeserializer implements JsonDeserializer<GroupBy> {

  @Override
  public GroupBy deserialize(JsonElement jsonElement, Type type,
      JsonDeserializationContext jsonDeserializationContext) {

    JsonObject obj = jsonElement.getAsJsonObject();
    GroupByType groupByType = GroupByType.fromString(obj.get("name").getAsString());

    GroupBy result = null;

    switch (groupByType) {
      case TAGS:
        result = deserializeGroupByTags(obj);
        break;
      case TIME:
        result = deserializeGroupByTime(obj);
        break;
      case VALUE:
        result = deserializeGroupByValue(obj);
        break;
      case BIN:
        result = deserializeGroupByBin(obj);
        break;
      default:
        break;
    }

    return result;
  }

  private GroupBy deserializeGroupByTags(JsonObject groupByObj) {
    GroupByTags result = new GroupByTags();
    JsonArray tags = groupByObj.getAsJsonArray("tags");
    tags.forEach(tag -> result.addTag(tag.getAsString()));
    return result;
  }

  private GroupBy deserializeGroupByTime(JsonObject groupByObj) {
    GroupByTime result = new GroupByTime();
    result.setGroupCount(groupByObj.get("group_count").getAsString());
    JsonObject rangeSize = groupByObj.get("range_size").getAsJsonObject();
    result.setRangeSize(rangeSize.get("value").getAsInt(), rangeSize.get("unit").getAsString());
    return result;
  }

  private GroupBy deserializeGroupByValue(JsonObject groupByObj) {
    GroupByValue result = new GroupByValue();
    result.setRangeSize(groupByObj.get("range_size").getAsLong());
    return result;
  }

  private GroupBy deserializeGroupByBin(JsonObject groupByObj) {
    GroupByBin result = new GroupByBin();
    JsonArray bins = groupByObj.getAsJsonArray("bins");
    bins.forEach(bin -> result.addBin(bin.getAsString()));
    return result;
  }

}
