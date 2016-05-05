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
    /**
     * 这些常亮是Storm组件的唯一表示符
     */
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

        /**
         * 注册一个sentence spout并且赋值给其唯一ID；
         * 2表示设置SentenceSpout并发为两个task，每个task指派各自的excutor线程；
         */
        builder.setSpout(SENTENCE_SPOUT_ID, spout, 2);

        /**
         * 注册SplitBolt，并且返回BoltDeclarer；
         * 这个bolt订阅SentenceSpout发射出来的数据流；
         * 通过SentenceSpout的唯一ID赋值给BoltDeclarer.shuffleGrouping()方法确立了这种订阅关系；
         * shuffleGrouping()方法告诉storm，要将SentenceSpout发射的tuple随机分发给SplitSentenceBolt的实例；
         * 2 表示两个executor数量；4 表示task的数量，即每个executor线程指派2个task来执行
         */
        builder.setBolt(SPLIT_BOLD_ID, splitBolt, 2).setNumTasks(4).shuffleGrouping(SENTENCE_SPOUT_ID);

        /**
         *BoltDeclarer的fieldsGrouping()方法保证所有的"word"字段值相同的tuple会被路由到同一个WordCountBolt实例中；
         * 4 表示4个executor，每个执行一个task
         */
        builder.setBolt(COUNT_BOLD_ID,countBolt, 4).fieldsGrouping(SPLIT_BOLD_ID, new Fields("word"));

        /**
         * BoltDeclarer的globalGrouping()方法保证所有的tuple路由到唯一的ReportBolt任务中。
         */
        builder.setBolt(REPORT_BOLD_ID, reportBolt).globalGrouping(COUNT_BOLD_ID);

        /**
         * Config()类是HashMap<String, Object>的子类，里面定义了一些Storm特有的常量和方法；
         * 用来配置topology运行时的行为，最终分发给spout的open()、blot的prepare()方法；
         */
        Config config = new Config();

        /**
         * 增加分配给一个topology的workers数量
         */
        config.setNumWorkers(2);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        Utils.waitForSeconds(10);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();


    }
}
