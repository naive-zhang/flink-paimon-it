package com.fishsun.bigdata.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;

public class EnvUtils {
    protected static final Logger logger = LoggerFactory.getLogger(EnvUtils.class);
    protected static StreamExecutionEnvironment env;
    protected static StreamTableEnvironment tableEnv;
    protected static long checkpointInterval;
    protected static HiveCatalog hiveCatalog;

    public static StreamExecutionEnvironment getStreamEnv() {
        return env;
    }

    public static StreamTableEnvironment getTableEnv() {
        return tableEnv;
    }

    public static void setupStreamEnv(boolean isInLocalMode) {
        checkpointInterval = 10 * 1000L;
        Configuration conf = new Configuration();
        //设置WebUI绑定的本地端口
        conf.setString(RestOptions.BIND_PORT, "8090-8100");
        conf.setString("table.exec.sink.upsert-materialize", "NONE");
        FileUtils.clearDir(FileUtils.getPipelineIOCachePath(false), true);
        conf.setString("taskmanager.tmp.dirs", FileUtils.getPipelineIOCachePath(false));
        // 设置执行环境
        if (isInLocalMode) {
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        } else {
            env = StreamExecutionEnvironment.createLocalEnvironment();
        }
        env.setMaxParallelism(1);
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(checkpointInterval);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        FileUtils.clearDir(FileUtils.getCheckpointPath(false), true);
        env.getCheckpointConfig().setCheckpointStorage("file://" + FileUtils.getCheckpointPath());
        tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));


        // Create a source table
        tableEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.STRING())
                        .column("create_time", DataTypes.TIMESTAMP(3))
                        .build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
                .build());
        registerHiveCatalog();
        // registerPaimonCatalog();
        // registerDataGen();
        // registerPaimonHiveCatalog();
    }

    public static void registerHiveCatalog() {
        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = FileUtils.getHiveConfDir(false);

        hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hiveCatalog);
// set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");
    }
}
