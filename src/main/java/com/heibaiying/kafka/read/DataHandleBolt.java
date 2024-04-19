package com.heibaiying.kafka.read;

import com.alibaba.fastjson.JSONArray;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DataHandleBolt extends BaseRichBolt {

    private OutputCollector collector;
    private static final String DIRECTORY_PATH = "src/main/resources/handled_data";
//    private static final String DIRECTORY_PATH = "/home/huazhao/ClusterWork/handled_data";
    private static final long MAX_FILE_SIZE = 1024 * 1024 * 300; // 300MB
    private static int fileIndex = 0;
    private static File file = new File(DIRECTORY_PATH, "CleanData" + fileIndex + ".json");
//    private static FileWriter writer;
    private BufferedWriter writer = null;
    private Set<String> modelIds = new HashSet<>();

    private long success = 0L;
    private long failure = 0L;


    // 在类中定义 StringBuilder 用于累积字符串
    private StringBuilder contentBuilder = new StringBuilder();
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        prepareHdfs(stormConf, context, collector);
    }

//    public void execute(Tuple input) {
//        try {
//            String value = processJson(input.getStringByField("value"));
//            if (value != null) {
//                success++;
//                System.out.println("JSON processing successful: " + success + "values is :  " + value.length());
////                Content += value + ",\n";
//                System.out.println("JSON processing successful: " + success + "values is :  " + value.length());
//                // 使用 StringBuilder 来拼接字符串
//                contentBuilder.append(value).append(",\n");
////                writeToFile(value);
//                if (success == 70590) {
//                    writeToFile(contentBuilder.toString());
//                    contentBuilder.setLength(0);
//                }
//            }
//            collector.ack(input);
//        } catch (Exception e) {
//            e.printStackTrace();
//            collector.fail(input);
//        }
//    }

    private String processJson(String jsonString) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode rootNode = mapper.readTree(jsonString);
            String modelId = rootNode.path("modelId").asText(null);
            if (modelId == null || modelIds.contains(modelId)) {
                failure++;
                System.out.println("theSameModel failed: " + failure);
                return null; // Skip if modelId is missing or already processed
            }

            modelIds.add(modelId); // Track modelId to prevent duplicates
            if (!rootNode.hasNonNull("tags") || !rootNode.hasNonNull("library_name") || !rootNode.hasNonNull("pipeline_tag")) {
                failure++;
                System.out.println("hasNonNull failed: " + failure);
                return null;
            }

            // Remove specified fields
            if (rootNode instanceof ObjectNode) {
                ObjectNode objectNode = (ObjectNode) rootNode;
                objectNode.remove("tags");
                objectNode.remove("library_name");
                objectNode.remove("pipeline_tag");
            }

            // Default values for missing required fields
            if (!rootNode.hasNonNull("author")) {
                ((ObjectNode) rootNode).put("author", "unknown");
            }

            // Convert the modified rootNode back to a JSON string
            return mapper.writeValueAsString(rootNode);
        } catch (Exception e) {
            System.out.println("Error processing JSON: " + e.getMessage());
            return null;
        }
    }

    private boolean isFirstJson = true; // To check if it's the first JSON object in the file

    private void writeToFile(String content) throws IOException {
        // Check if writer needs to be reset
        if (writer == null || file.length() > MAX_FILE_SIZE) {
            // Close previous writer if exists
            closeCurrentWriter();

            // Increment file index and create a new file path
            fileIndex++;
            file = new File(DIRECTORY_PATH, "CleanData" + fileIndex + ".json");
            // Create BufferedWriter with append mode and buffered performance
            writer = Files.newBufferedWriter(Paths.get(file.getAbsolutePath()),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND);
            writer.write("[\n"); // Start new JSON array
            isFirstJson = true; // Reset the first JSON flag
        }

        // Write content
        if (!isFirstJson) {
            writer.write(",\n"); // Add comma before next JSON object if it's not the first
        } else {
            isFirstJson = false; // No comma needed for the first JSON object
        }

        writer.write(content); // Write the JSON content
        writer.write("]\n"); // Close the JSON object
        // Flush the writer to ensure immediate write to disk
        writer.flush();
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields to declare in this bolt
    }

    private void closeCurrentWriter() throws IOException {
        if (writer != null) {
            writer.write("]\n"); // Close previous JSON array

            writer.close();
            writer = null; // Reset writer to null after close
        }
    }


    private static final String HDFS_PATH = "hdfs://node1:9000";
    private static final String HDFS_USER = "root";
    private static FileSystem fileSystem = null;


    public void prepareHdfs(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", HDFS_PATH);
            configuration.set("dfs.replication", "1");
            fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, HDFS_USER);
            mkDir("/user/storm_processed/test/");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void mkDir(String path) throws IOException {
        Path hdfsPath = new Path(path);
        if (!fileSystem.exists(hdfsPath)) {
            fileSystem.mkdirs(hdfsPath);
        }
    }


    private void writeToHDFS(String content, String filePath) throws IOException {
        Path path = new Path(filePath);
        FSDataOutputStream out = null;
        if (fileSystem.exists(path)) {
            // Append to file if it already exists
            out = fileSystem.append(path);
        } else {
            // Create file if it does not exist
            out = fileSystem.create(path, true);
        }
        try {
            out.writeBytes(content);
            out.flush(); // Ensure all data is sent to HDFS
        } finally {
            IOUtils.closeStream(out); // Safely close the output stream
        }
    }

    public void execute(Tuple input) {
        try {
            String value = processJson(input.getStringByField("value"));
            if (value != null) {
                success++;
                // Build content to write
                String contentToWrite = success == 1000 ? contentBuilder.toString() : value + ",\n";
                if (success == 1000) {
                    contentBuilder.setLength(0); // Reset the StringBuilder
                }
                writeToHDFS(contentToWrite, "/user/storm_processed/test/CleanData" + fileIndex + ".json");
            }
            collector.ack(input);
        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(input);
        }
    }
}
