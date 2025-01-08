package com.fishsun.bigdata;

import com.fishsun.bigdata.utils.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;

public class PaimonBasicTestSuite {
    protected static final Logger logger = LoggerFactory.getLogger(PaimonBasicTestSuite.class);
    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tableEnv;
    protected long checkpointInterval;

    @Before
    public void setUp() {
        checkpointInterval = 10 * 1000L;
        Configuration conf = new Configuration();
        //设置WebUI绑定的本地端口
        conf.setString(RestOptions.BIND_PORT, "8090-8100");
        conf.setString("table.exec.sink.upsert-materialize", "NONE");
        FileUtils.clearDir(FileUtils.getPipelineIOCachePath(false));
        conf.setString("taskmanager.tmp.dirs", FileUtils.getPipelineIOCachePath(false));
        // 设置执行环境
        env = StreamExecutionEnvironment.createLocalEnvironment(conf);
        env.setMaxParallelism(1);
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(checkpointInterval);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        FileUtils.clearDir(FileUtils.getCheckpointPath(false));
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
        registerDataGen();
        registerPaimonCatalog();
    }

    private void registerDataGen() {
        tableEnv.executeSql("create database test");
        tableEnv.executeSql("CREATE TABLE if not exists default_catalog.test.datagen1 (\n" +
                "  `id` Int PRIMARY KEY NOT ENFORCED,\n" +
                "  `name` String,\n" +
                "  `age` Int,\n" +
                "  `create_time` Timestamp\n" +
                ") with (\n" +
                "'connector' = 'datagen',\n" +
                "'fields.id.kind' = 'random',\n" +
                "'fields.id.min' = '1',\n" +
                "'fields.id.max' = '10',\n" +
                "'fields.name.length' = '10',\n" +
                "'fields.age.min' = '18',\n" +
                "'fields.age.max' = '60',\n" +
                "'rows-per-second' = '3'\n" +
                ");");
    }

    private void registerPaimonCatalog() {
        String catalogCreateDdl = "CREATE CATALOG paimon WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'warehouse' = 'file://" + FileUtils.getLakehouseDefaultPath() + "'\n" + //'hdfs://dd-xian-0103-001:8020/lakehouse'\n" +
                "    );";
        System.out.println(catalogCreateDdl);
        tableEnv.executeSql(catalogCreateDdl);
        tableEnv.executeSql("use catalog paimon");
        tableEnv.executeSql("create database if not exists test");
    }
}
