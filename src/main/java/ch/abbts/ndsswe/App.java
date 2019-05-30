package ch.abbts.ndsswe;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Hello world!
 */
public class App {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("clickstream", new ClickstreamSpout(), 10);
        builder.setBolt("extract1", new RefererExtraction(), 3).shuffleGrouping("clickstream");
        builder.setBolt("count", new RefererCount(), 2).shuffleGrouping("extract1");

        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            // let it run for 1 min.
            Utils.sleep(60 * 1000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
