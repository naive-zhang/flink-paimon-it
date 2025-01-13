package com.fishsun;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;


public class HiveMetastoreStarter {
    public static void main(String[] args) throws Exception {
        // // 设置Hive配置
        // HiveConf hiveConf = new HiveConf();
        // hiveConf.set("hive.metastore.uris", "thrift://localhost:9083");
        // hiveConf.set("hive.metastore.warehouse.dir", "file:///tmp/hive/warehouse");
        // hiveConf.set("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:metastore_db;create=true");
        // hiveConf.set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");
        // hiveConf.setBoolean("hive.metastore.schema.verification", false);
        // hiveConf.setBoolean("hive.metastore.schema.autocreate.all", true);
        // hiveConf.set("hive.exec.scratchdir", "/tmp/hive");
        // hiveConf.set("hive.querylog.location", "/tmp/hive/query.log");
//
        // // 配置 Hadoop Thrift Auth Bridge
        // HadoopThriftAuthBridge bridge = new HadoopThriftAuthBridgeImpl();
//
        // // 获取Metastore端口
        // int port = hiveConf.getIntVar(HiveConf.ConfVars.METASTORE_PORT);
//
        // // 创建 Hadoop Configuration
        // Configuration hadoopConf = new Configuration();
        // hadoopConf.addResource(hiveConf);
//
        // // 启动Metastore
        // System.out.println("Starting Hive Metastore on port " + port + "...");
        // try {
        //     // 使用新的 startMetaStore 方法签名
        //     MetaStore.startMetaStore(port, bridge, hadoopConf);
        //     System.out.println("Hive Metastore started on port " + port);
        // } catch (MetaException e) {
        //     System.err.println("Failed to start Hive Metastore: " + e.getMessage());
        //     e.printStackTrace();
        //     System.exit(1);
        // }
//
        // // 保持主线程运行
        // Thread.currentThread().join();
    }
}
