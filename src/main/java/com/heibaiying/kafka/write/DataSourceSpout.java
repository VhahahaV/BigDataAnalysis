package com.heibaiying.kafka.write;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.heibaiying.kafka.entity.ModelObject;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.stream.Collectors;

public class DataSourceSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private int index = 0;
    private JSONArray array = new JSONArray();

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        try {
            // 使用ClassLoader获取资源流
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("hf_metadata.json");
            String jsonString = new BufferedReader(new InputStreamReader(inputStream))
                    .lines().collect(Collectors.joining("\n"));
            array = JSONArray.parseArray(jsonString);
        } catch (Exception e) {
            System.err.println("Failed to read or parse file");
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        if (array.isEmpty()) {
            System.out.println("No data to emit, sleeping...");
            Utils.sleep(1000);
            return;
        }
        if (index >= array.size()) {
            index = 0;
        }
        Object obj = array.get(index++);
        String data = JSON.toJSONString(obj);
        spoutOutputCollector.emit(new Values("modelObject", data));
        Utils.sleep(1000);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("key", "message"));
    }
}
