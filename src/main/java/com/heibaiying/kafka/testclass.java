package com.heibaiying.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.heibaiying.kafka.entity.ModelObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;


public class testclass {
//    public static void main(String[] args) {
//        File file = new File("src/main/resources/hf_metadata.json");
//        try {
//            nioMethod(file);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//    private static void nioMethod(File file) throws IOException {
//        String jsonString = new String(Files.readAllBytes(Paths.get(file.getPath())));
//
//        JSONArray array = JSONArray.parseArray(jsonString);
//        List<ModelObject> modelObjects = JSON.parseArray(jsonString, ModelObject.class);
//        System.out.println(JSON.toJSONString(modelObjects.get(0).getDownloads()));
//
//    }
    public static void main(String[] args) {
        String jsonString = "[{\"modelId\":\"1\",\"downloads\":100},{\"modelId\":\"2\",\"downloads\":200}]";
        try {
            prepareHdfs(null, null, null);
            writeToHDFS(jsonString, "/user/storm_processed/test/test.json");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static final String HDFS_PATH = "hdfs://node1:9000";
    private static final String HDFS_USER = "root";
    private static FileSystem fileSystem = null;


    private static void prepareHdfs(Map stormConf, TopologyContext context, OutputCollector collector) {
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


    private static void writeToHDFS(String content, String filePath) throws IOException {
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



}
