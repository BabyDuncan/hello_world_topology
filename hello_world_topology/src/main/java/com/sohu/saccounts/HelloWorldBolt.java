package com.sohu.saccounts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * User: guohaozhao (guohaozhao116008@sohu-inc.com)
 * Date: 4/2/14 16:48
 * the simplest bolt
 */
public class HelloWorldBolt extends BaseRichBolt {

    private int myCount;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // doing nothing...
    }

    @Override
    public void execute(Tuple tuple) {
        String test = tuple.getStringByField("sentence");
        if ("Hello World".equals(test)) {
            myCount++;
            System.out.println("Found a Hello World! My Count is now: "
                    + Integer.toString(myCount));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // doing nothing...
    }
}
