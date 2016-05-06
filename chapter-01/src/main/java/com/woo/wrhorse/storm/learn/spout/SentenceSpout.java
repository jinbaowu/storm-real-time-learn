package com.woo.wrhorse.storm.learn.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.woo.wrhorse.storm.learn.utile.Utils;

import java.util.Map;

/**
 * Created by jinbao.wu on 2016/5/5.
 */
public class SentenceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private String[] sentences = {
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't have a cow man",
            "i don't think i like fleas"
    };
    private int index = 0;

    /**
     * 每个spout和bolt都需要实现的方法；
     * 告诉storm该方法会发送哪些数据流；
     * 每个数据流的tuple中包含哪些字段；
     * 本例中，声明tuple包含一个字段（"sectence"）
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    /**
     * 所有spout组件在初始化时，优先调用；
     * 三个参数：一个包含Storm配置信息的map；topolopy中组件的信息；SpoutOutputCollector提供发射tuple的方法。
     * 本例中，仅仅实现将SpoutOutputCollector对象的引用保存在变量中。
     * @param map
     * @param topologyContext
     * @param collector
     */

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;

    }

    /**
     * 所有spout实现的核心所在；
     * Storm通过此方法，向输出的collector发射tuple
     */
    @Override
    public void nextTuple() {
//        this.collector.emit(new Values(sentences[index]));
//        index++;
//        if (index >= sentences.length) {
//            index = 0;
//        }
        if(index < sentences.length) {
            this.collector.emit(new Values(sentences[index]));
            index++;
        }
        Utils.waitForMillis(1);
    }
}
