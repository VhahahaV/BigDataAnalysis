package com.heibaiying.kafka.read;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DataHandleBolt extends BaseRichBolt {

    private OutputCollector collector;
//    private static final String DIRECTORY_PATH = "src/main/resources/handled_data";
    private static final String DIRECTORY_PATH = "/home/huazhao/ClusterWork/handled_data";
    private static final long MAX_FILE_SIZE = 1024 * 1024; // 1MB
    private static int fileIndex = 1;
    private static File file = new File(DIRECTORY_PATH, "CleanData" + fileIndex + ".json");
    private static FileWriter writer;
    private Set<String> modelIds = new HashSet<>();

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            ensureFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple input) {
        try {
            String value = input.getStringByField("value");
            if (processJson(value)) {
                writeToFile(value);
            } else {
                System.out.println("JSON does not meet the requirements or is a duplicate.");
            }
            collector.ack(input);
        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(input);
        }
    }

    private boolean processJson(String jsonString) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode rootNode = mapper.readTree(jsonString);
            String modelId = rootNode.path("modelId").asText(null);
            if (modelId == null || modelIds.contains(modelId)) {
                return false; // Skip if modelId is missing or already processed
            }

            modelIds.add(modelId); // Track modelId to prevent duplicates
            if (!rootNode.hasNonNull("tags") || !rootNode.hasNonNull("library_name") || !rootNode.hasNonNull("pipeline_tag")) {
//                if (!rootNode.hasNonNull("tags")) ((ObjectNode) rootNode).putArray("tags");
//                if (!rootNode.hasNonNull("library_name")) ((ObjectNode) rootNode).put("library_name", "unknown");
//                if (!rootNode.hasNonNull("pipeline_tag")) ((ObjectNode) rootNode).put("pipeline_tag", "unknown");
                return false;
            }

            // Default values for missing required fields
            if (!rootNode.hasNonNull("author")) ((ObjectNode) rootNode).put("author", "unknown");
            return true;
        } catch (Exception e) {
            System.out.println("Error processing JSON: " + e.getMessage());
            return false;
        }
    }

    private void writeToFile(String content) throws IOException {
        if (writer == null || file.length() > MAX_FILE_SIZE) {
            if (writer != null) {
                writer.close();
            }
            file = new File(DIRECTORY_PATH, "CleanData" + (fileIndex++) + ".json");
            writer = new FileWriter(file, true);
        }
        writer.write(content + System.lineSeparator());
        writer.flush();
    }

    private void ensureFile() throws IOException {
        File directory = new File(DIRECTORY_PATH);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        file = new File(directory, "CleanData" + fileIndex + ".json");
        if (!file.exists()) {
            file.createNewFile();
        }
        writer = new FileWriter(file, true);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields to declare in this bolt
    }

    @Override
    public void cleanup() {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
