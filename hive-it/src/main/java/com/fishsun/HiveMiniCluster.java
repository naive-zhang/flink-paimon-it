package com.fishsun;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.datanucleus.store.schema.SchemaTool;

public class HiveMiniCluster {

    public static void main(String[] args) {
        // 1. 清理旧的 Metastore 数据
        try {
            MetastoreCleaner.cleanMetastore();
        } catch (Exception e) {
            System.err.println("Error cleaning Metastore: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        // 2. 配置 HiveConf（从 hive-site.xml 加载）
        HiveConf hiveConf = new HiveConf();
        // HiveConf 自动加载类路径中的 hive-site.xml

        // 3. 初始化 Metastore Schema
        try {
            System.out.println("Initializing Hive Metastore schema...");
            SchemaTool.main(new String[]{"-initSchema", "-dbType", "h2"});
            System.out.println("Hive Metastore schema initialized successfully.");
        } catch (Exception e) {
            System.err.println("Error initializing Hive Metastore schema: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        // 4. 验证 Metastore 初始化
        try {
            HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
            String version = client.getTokenStrForm();
            System.out.println("Hive Metastore schema version: " + version);
        } catch (MetaException e) {
            System.err.println("MetaException: " + e.getMessage());
            e.printStackTrace();
            return;
        } catch (Exception e) {
            System.err.println("Exception: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        // 5. 启动 HiveServer2
        try {
            HiveServer2 hiveServer2 = new HiveServer2();
            hiveServer2.init(hiveConf);
            hiveServer2.start();
            System.out.println("HiveMiniCluster started successfully.");
        } catch (Exception e) {
            System.err.println("Error starting HiveServer2: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
