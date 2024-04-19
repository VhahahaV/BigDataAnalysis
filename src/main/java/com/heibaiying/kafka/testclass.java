package com.heibaiying.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.heibaiying.kafka.entity.ModelObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


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

        prepare();
        try {
            mkDir();
            create();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final String HDFS_PATH = "hdfs://10.119.12.221:9000";
    private static final String HDFS_USER = "root";
    private static FileSystem fileSystem;

    private static void prepare() {
        try {
            Configuration configuration = new Configuration();
            // 这里我启动的是单节点的 Hadoop,所以副本系数设置为 1,默认值为 3
            configuration.set("dfs.replication", "3");
                fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, HDFS_USER);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    private static void mkDir() throws Exception {
        fileSystem.mkdirs(new Path("/storm_processed/"));
    }

    private static void create() throws Exception {
        // 如果文件存在，默认会覆盖, 可以通过第二个参数进行控制。第三个参数可以控制使用缓冲区的大小
        FSDataOutputStream out = fileSystem.create(new Path("/storm_processed/test/a.txt"),
                true, 4096);
        out.write("hello hadoop!".getBytes());
        out.write("hello spark!".getBytes());
        out.write("hello flink!".getBytes());
        // 强制将缓冲区中内容刷出
        out.flush();
        out.close();
    }

}
