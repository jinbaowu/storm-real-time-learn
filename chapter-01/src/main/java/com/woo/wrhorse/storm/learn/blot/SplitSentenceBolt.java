package com.woo.wrhorse.storm.learn.blot;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by jinbao.wu on 2016/5/5.
 */
public class SplitSentenceBolt extends BaseRichBolt {
    private OutputCollector collector;

    /**
     * Bolt初始化时调用；
     * 用于准备bolt用到的资源；
     * 本例中，也只是保存OutputCollector对象的引用
     * @param map
     * @param topologyContext
     * @param collector
     */
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;

    }

    /**
     * Bolt的核心功能在此方法中实现；
     * 每当从订阅的数据中就收一个tuple，都会调用此方法；
     * 本例中，此方法按照字符串读取"sentence"字段的值，拆分，然后向后面的输出流发射一个tuple
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getStringByField("sentence");
        String[] words = sentence.split(" ");
        for(String word: words){
            this.collector.emit(new Values(word));
        }

    }

    /**
     * 声明一个输出流；
     * 每个tuple包含一个字段word
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));

    }
}
