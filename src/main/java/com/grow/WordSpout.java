package com.grow;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * spout用于数据生成的组件
 */
public class WordSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private String[] words = {"hello","world","storm","study"};//单词池
    private int index = 0;

    /**
     * 初始化spout
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    /**
     * 通过收集器发出生成的数据
     */
    public void nextTuple() {
        this.collector.emit(new Values(this.words[index]));
        index++;
        if(index>=words.length){
            index = 0;
        }

        //等待500ms
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 声明改tuple的输出模式
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}