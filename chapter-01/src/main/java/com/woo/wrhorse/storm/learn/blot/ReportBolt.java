package com.woo.wrhorse.storm.learn.blot;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.*;

/**
 * Created by jinbao.wu on 2016/5/5.
 */
public class ReportBolt extends BaseRichBolt {
    private HashMap<String,Long> counts = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.counts = new HashMap<String, Long>();

    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counts.put(word, count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    /**
     * Strom在终止前会调用这个方法；
     * 通常，此方法释放bolt占用的资源
     */
    @Override
    public void cleanup() {
        System.out.println("--- Final COUNTS ---");
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);
        for (String key: keys){
            System.out.println(key + ": " + this.counts.get(key));
        }
        System.out.println("--------------------");
    }
}
