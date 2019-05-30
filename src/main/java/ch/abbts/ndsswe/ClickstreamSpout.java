package ch.abbts.ndsswe;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class ClickstreamSpout extends BaseRichSpout {

    SpoutOutputCollector collector;
    BufferedReader br;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
        try {
            br = new BufferedReader(new FileReader(("/Volumes/Videos/DataScience/clickstream/2015_01_en_clickstream.tsv")));
        } catch (FileNotFoundException e) {
            System.out.println("ERROR -- Could not open input stream");;
            System.exit(1);
        }
    }

    public void nextTuple() {
        // in order to see whats happens we slow down the processing
        Utils.sleep(1000);
        try {
            String line = br.readLine();
            collector.emit(new Values(line));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("clicks"));
    }
}
