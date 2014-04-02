package com.sohu.saccounts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * User: guohaozhao (guohaozhao116008@sohu-inc.com)
 * Date: 4/2/14 16:01
 * the simplest spout
 */
public class HelloWorldSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private int referenceRandom;
    private static final int MAX_RANDOM = 10;

    /**
     * 随机构造函数
     */
    public HelloWorldSpout() {
        final Random rand = new Random();
        referenceRandom = rand.nextInt(MAX_RANDOM);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        final Random rand = new Random();
        int instanceRandom = rand.nextInt(MAX_RANDOM);
        if (instanceRandom == referenceRandom) {
            collector.emit(new Values("Hello World"));
        } else {
            collector.emit(new Values("Other Random Word"));
        }
    }
}
