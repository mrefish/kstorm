package quux00.wordcount.kafka;

import quux00.wordcount.acking.ReportBolt;
import quux00.wordcount.acking.SimpleBolt;
import quux00.wordcount.acking.SplitSentenceBolt;
import quux00.wordcount.acking.WordCountBolt;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import backtype.storm.Config;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import java.util.Properties;
import storm.kafka.trident.TridentKafkaState;

public class WordCountAckedTopology {

  private static final String SENTENCE_SPOUT_ID = "kafka-sentence-spout";
  private static final String SIMPLE_BOLT_ID = "acking-simple-bolt";
  private static final String TOPOLOGY_NAME = "acking-word-count-topology";
  private static final String TOPIC = "sentences"

  public static void main(String[] args) throws Exception {
    int numSpoutExecutors = 1;

    // make kafka spout, simple bolt, and kafka bolt
    KafkaSpout kspout = buildKafkaSentenceSpout();
    SimpleBolt simpleBolt = new SimpleBolt();
    KafkaBolt bolt = new KafkaBolt()
      .withTopicSelector(new DefaultTopicSelector(TOPIC))
      .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "message"));

    Config conf = new Config();

    //set kafka producer properties.
    Properties props = new Properties();
    props.put("metadata.broker.list", "localhost:9092");
    props.put("request.required.acks", "1");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SENTENCE_SPOUT_ID, kspout, numSpoutExecutors);
    builder.setBolt(SIMPLE_BOLT_ID, simpleBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
    builder.setBolt("forwardToKafka", bolt, 1).shuffleGrouping(SIMPLE_BOLT_ID);

    StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
  }

  private static KafkaSpout buildKafkaSentenceSpout() {
    String zkHostPort = "localhost:2181";
    String topic = TOPIC;

    String zkRoot = "/acking-kafka-sentence-spout";
    String zkSpoutId = "acking-sentence-spout";
    ZkHosts zkHosts = new ZkHosts(zkHostPort);

    SpoutConfig spoutCfg = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId);
    KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);
    return kafkaSpout;
  }
}
