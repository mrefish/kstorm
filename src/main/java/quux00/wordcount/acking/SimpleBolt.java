package quux00.wordcount.acking;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SimpleBolt extends BaseRichBolt {

  // private static final long serialVersionUID = 6843364678084556655L;
  // private static final Long ONE = Long.valueOf(1);

  private OutputCollector collector;
  // private Map<String, Long> countMap;

  @Override @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector outCollector) {
    collector = outCollector;
    // countMap = new HashMap<>();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("key", "message"));
  }

  @Override
  public void execute(Tuple tuple) {
    Object value = tuple.getValue(0);
    String sentence = null;
    if (value instanceof String) {
      sentence = (String) value;

    } else {
      // Kafka returns bytes
      byte[] bytes = (byte[]) value;
      try {
        sentence = new String(bytes, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }
    collector.emit(tuple, new Values("key", sentence));
    collector.ack(tuple);
  }
}
