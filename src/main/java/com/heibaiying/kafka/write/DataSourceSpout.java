package com.heibaiying.kafka.write;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.heibaiying.kafka.entity.ModelObject;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * 产生词频样本的数据源
 */
public class DataSourceSpout extends BaseRichSpout {

//    private List<String> list = Arrays.asList("Spark", "Hadoop", "HBase", "Storm", "Flink", "Hive");
    File metaFile = new File("src/main/resources/hf_metadata.json");
    List<ModelObject> modelObjects = new ArrayList<>();
    private SpoutOutputCollector spoutOutputCollector;
    private int index = 0; // 用于跟踪当前发送的数据索引

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        // 读取JSON数据到modelObjects列表中
        try {
            String jsonString = new String(Files.readAllBytes(Paths.get(metaFile.getPath())));
            JSONArray array = JSONArray.parseArray(jsonString);
            this.modelObjects = JSON.parseArray(jsonString, ModelObject.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        // 模拟产生数据
        if (index >= modelObjects.size()) {
            index = 0; // 如果达到列表末尾，重新开始
        }
        ModelObject modelObject = modelObjects.get(index++);
        // 调用productData将ModelObject格式化为可发送的数据
        String data = productData(modelObject);
        spoutOutputCollector.emit(new Values("modelObject", data));
        Utils.sleep(1000);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare( new Fields("key", "message"));
    }


    /**
     * 模拟数据 - 这里我们简单地将ModelObject转换为JSON字符串
     */
    private String productData(ModelObject modelObject) {
        return JSON.toJSONString(modelObject);
    }



}