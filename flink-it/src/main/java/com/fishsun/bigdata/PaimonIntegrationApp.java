package com.fishsun.bigdata;

import com.fishsun.bigdata.utils.EnvUtils;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PaimonIntegrationApp {
    public static void main(String[] args) {
        EnvUtils.setupStreamEnv(true);
        StreamTableEnvironment tableEnv = EnvUtils.getTableEnv();
        String[] databases = tableEnv.listDatabases();
        for (String database : databases) {
            System.out.println(database);
        }
    }
}
