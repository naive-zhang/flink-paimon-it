package com.fishsun.utils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class FileUtils {

    /**
     * 不同的pc上面获得不同的路径
     * @return
     */
    public static String getWarehousePath() {
        return "file:///Users/zhangxinsen/workspace/ane/flink-paimon-it/lakehouse";
    }

    /**
     * 解析文件中的 json 内容
     *
     * @param filePath
     * @return
     */
    public static Map<String, String> parseJsonFromFile(String filePath) {
        Map<String, String> map = new HashMap<>();
        try {
            // 读取文件内容
            String json = new String(Files.readAllBytes(Paths.get(filePath)));
            System.out.println(json);
            Gson gson = new Gson(); // 创建 Gson 实例
            // 指定 Map<String, String> 的类型
            Type type = new TypeToken<Map<String, String>>() {
            }.getType();
            // 解析 JSON 为 Map
            map = gson.fromJson(json, type);
            // 打印 Map 内容
            for (Map.Entry<String, String> entry : map.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }
        } catch (IOException e) {
            // 处理文件读取错误
            System.out.println("文件读取错误: " + e.getMessage());
            e.printStackTrace();
        } catch (com.google.gson.JsonSyntaxException e) {
            // 处理 JSON 解析错误
            System.out.println("JSON 解析错误: " + e.getMessage());
            e.printStackTrace();
        }
        return map;
    }
}
