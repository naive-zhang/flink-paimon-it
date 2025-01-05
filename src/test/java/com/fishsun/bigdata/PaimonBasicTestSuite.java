package com.fishsun.bigdata;

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

import java.time.ZoneId;

public class PaimonBasicTestSuite {
    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tableEnv;

    @Before
    public void setUp() {
        // 创建 Flink 流执行环境和 SQL 表环境
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 9999);
        env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        // 配置
        env.setMaxParallelism(2);
        // env.getConfig().addDefaultKryoSerializer(MyCustomType.class, CustomKryoSerializer.class);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(2);
        // 10s for checkpoint
        env.getCheckpointConfig().setCheckpointInterval(10 * 1000L);
        // set configuration early
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
        tableEnv.executeSql("CREATE CATALOG paimon WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'warehouse' = 'hdfs://dd-xian-0103-001:8020/lakehouse'\n" +
                "    );");
        tableEnv.executeSql("use catalog paimon");
        tableEnv.executeSql("create database if not exists test");
    }
}
