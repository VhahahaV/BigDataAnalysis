package com.heibaiying.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.heibaiying.kafka.entity.ModelObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class testclass {
    public static void main(String[] args) {
        File file = new File("src/main/resources/hf_metadata.json");
        try {
            nioMethod(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static void nioMethod(File file) throws IOException {
        String jsonString = new String(Files.readAllBytes(Paths.get(file.getPath())));

        JSONArray array = JSONArray.parseArray(jsonString);
        List<ModelObject> modelObjects = JSON.parseArray(jsonString, ModelObject.class);
        System.out.println(JSON.toJSONString(modelObjects.get(0).getDownloads()));

    }
}
