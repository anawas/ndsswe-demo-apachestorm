package ch.abbts.ndsswe;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * Hello world!
 */
public class App {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("clickstream", new ClickstreamSpout(), 10);
        builder.setBolt("extract1", new RefererExtraction(), 3).shuffleGrouping("clickstream");
        builder.setBolt("count", new RefererCount(), 2).fieldsGrouping("extract1", new Fields("referer"));

        Config conf = new Config();
        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        // let it run for 1 min.
        Utils.sleep(60 * 1000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
