package com.woo.wrhorse.storm.learn;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.woo.wrhorse.storm.learn.blot.ReportBolt;
import com.woo.wrhorse.storm.learn.blot.SplitSentenceBolt;
import com.woo.wrhorse.storm.learn.blot.WordCountBolt;
import com.woo.wrhorse.storm.learn.spout.SentenceSpout;
import com.woo.wrhorse.storm.learn.utile.Utils;

/**
 * Created by jinbao.wu on 2016/5/5.
 */
public class WordCountTopology {
    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLD_ID = "split-bolt";
    private static final String COUNT_BOLD_ID = "count-bolt";
    private static final String REPORT_BOLD_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) throws Exception{
        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SENTENCE_SPOUT_ID, spout);
        builder.setBolt(SPLIT_BOLD_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
        builder.setBolt(COUNT_BOLD_ID,countBolt).fieldsGrouping(SPLIT_BOLD_ID, new Fields("word"));
        builder.setBolt(REPORT_BOLD_ID, reportBolt).globalGrouping(COUNT_BOLD_ID);
        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        Utils.waitForSeconds(10);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();


    }
}
