package com.fishsun;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class HiveMetaStoreStarter {

    public static void main(String[] args) {
        try {
            // 初始化Hive配置
            HiveConf hiveConf = new HiveConf();

            // 分配一个随机的可用端口
            hiveConf.setIntVar(HiveConf.ConfVars.METASTORE_SERVER_PORT, 9083);
            hiveConf.set("hive.metastore.uris", "thrift://localhost:9083");

            // 使用内存中的Derby数据库，确保每次启动都是新的MetaStore
            hiveConf.set("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:metastore_db;create=true");
            hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, "file:///tmp/hive/warehouse");

            // 设置其他必要的配置
            hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, "localhost");
            hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_AUTO_CREATE_ALL, true);

            // 如果有需要配置的其他参数，也可以在这里设置
            hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_AUTO_CREATE_ALL, true);
            hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION, false);
//            hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION_RECORD_VERSION, false);


            // 创建 Thrift Auth Bridge
            // HadoopThriftAuthBridge bridge = new HadoopThriftAuthBridge();
            File confFile = new File("conf");
            if (!confFile.exists()) {
                confFile.mkdirs();
            }
            File outFile = new File("conf/hive-site.xml");
            try (FileOutputStream fos = new FileOutputStream(outFile)) {
                hiveConf.writeXml(fos);
                System.out.println("成功将配置信息写入 " + outFile.getAbsolutePath());
            } catch (IOException e) {
                e.printStackTrace();
            }

            // 启动 Hive MetaStore 服务
            // 注意，这里使用了带 (port, bridge, conf) 的签名
            HiveMetaStore.startMetaStore(9083, null, hiveConf);

            System.out.println("HiveMetaStore 已启动，监听端口: " + 9083);

            // 保持主线程运行，以保持MetaStore服务持续运行
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
