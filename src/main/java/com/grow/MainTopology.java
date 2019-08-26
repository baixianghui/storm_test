package com.grow;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MainTopology {
    public static void main(String[] args) throws TException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wordSpout", new WordSpout(), 1);
        builder.setBolt("countBolt", new CountBolt(), 1).shuffleGrouping("wordSpout");
        Config conf = new Config();

        // idea提交任务，读取本地Storm-core配置文件，提交集群
        // 读取本地storm-core包下的storm.yaml配置
        Map stormConf = Utils.readStormConfig();
        // 读取classpath下的配置文件
        // Map stormConf = Utils.findAndReadConfigFile("storm.yaml");
        List<String> seeds = new ArrayList<String>();
        seeds.add("47.103.19.138");
        stormConf.put(Config.NIMBUS_SEEDS, seeds);
        stormConf.put(Config.NIMBUS_THRIFT_PORT, 6627);
        stormConf.put(Config.WORKER_CHILDOPTS, "-Xmx8192m");
        stormConf.putAll(conf);
        System.out.println(stormConf);

        // 提交集群运行的jar
        String inputJar = "D:\\idea\\storm_test\\out\\artifacts\\storm_test_jar\\storm_test.jar";

        String uploadedJarLocation = StormSubmitter.submitJar(stormConf, inputJar);
        String jsonConf = JSONValue.toJSONString(stormConf);

        NimbusClient nimbus = NimbusClient.getConfiguredClient(stormConf);
        Nimbus.Iface client = nimbus.getClient();
        client.submitTopology("word_count", uploadedJarLocation, jsonConf, builder.createTopology());
//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        client.killTopology("word_count");

//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("word_count", config, builder.createTopology());
//        try {
//            Thread.sleep(waitSeconds * 1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        cluster.killTopology("word_count");
//        cluster.shutdown();
    }
}