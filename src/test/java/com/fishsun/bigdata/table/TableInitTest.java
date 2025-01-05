package com.fishsun.bigdata.table;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;

public class TableInitTest {
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tableEnv;
    private Table dataGenTable;
    private Table inputTable;

    @Before
    public void setUp() {
        // 创建 Flink 流执行环境和 SQL 表环境
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 配置
        // env.setMaxParallelism(1);
        // env.getConfig().addDefaultKryoSerializer(MyCustomType.class, CustomKryoSerializer.class);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
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
        dataGenTable = tableEnv.sqlQuery("select * from SourceTable");


        // create a DataStream
        DataStream<Row> dataStream = env.fromElements(
                Row.of("Alice", 12),
                Row.of("Bob", 10),
                Row.of("Alice", 100));

        // interpret the insert-only DataStream as a Table
        inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");
        tableEnv.createTemporaryView("input_table", inputTable);
    }

    @Test
    public void testSimpleSqlQuery() throws Exception {

        // Create a sink table (using SQL DDL)
//        tableEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') " +
//                "LIKE SourceTable (EXCLUDING OPTIONS) ");

        // Create a Table object from a SQL query
//        Table table2 = tableEnv.sqlQuery("SELECT * FROM SourceTable");

        // Emit a Table API result Table to a TableSink, same for SQL result
        // TableResult tableResult = table1.insertInto("SinkTable").execute();
        dataGenTable.execute().print();
//        tableEnv.toDataStream("SourceTable").print();
    }

    @Test
    public void testTable2stream() throws Exception {
        DataStream<Row> dataStream = tableEnv.toDataStream(inputTable);
        dataStream.print();
        env.execute();
    }

    @Test
    public void testTable2retractStream() throws Exception {
        Table aggTbl = tableEnv.sqlQuery("select name, sum(score) from input_table group by name");
        tableEnv.toChangelogStream(aggTbl)
                .print();
        env.execute();
    }

    @Test
    public void testTableJoin() throws Exception {

        // create a user stream
        DataStream<Row> userStream = env
                .fromElements(
                        Row.of(LocalDateTime.parse("2021-08-21T13:00:00"), 1, "Alice"),
                        Row.of(LocalDateTime.parse("2021-08-21T13:05:00"), 2, "Bob"),
                        Row.of(LocalDateTime.parse("2021-08-21T13:10:00"), 2, "Bob"))
                .returns(
                        Types.ROW_NAMED(
                                new String[]{"ts", "uid", "name"},
                                Types.LOCAL_DATE_TIME, Types.INT, Types.STRING));

        // create an order stream
        DataStream<Row> orderStream = env
                .fromElements(
                        Row.of(LocalDateTime.parse("2021-08-21T13:02:00"), 1, 122),
                        Row.of(LocalDateTime.parse("2021-08-21T13:07:00"), 2, 239),
                        Row.of(LocalDateTime.parse("2021-08-21T13:11:00"), 2, 999))
                .returns(
                        Types.ROW_NAMED(
                                new String[]{"ts", "uid", "amount"},
                                Types.LOCAL_DATE_TIME, Types.INT, Types.INT));

// create corresponding tables
        tableEnv.createTemporaryView(
                "UserTable",
                userStream,
                Schema.newBuilder()
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .column("uid", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .watermark("ts", "ts - INTERVAL '1' SECOND")
                        .build());

        tableEnv.createTemporaryView(
                "OrderTable",
                orderStream,
                Schema.newBuilder()
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .column("uid", DataTypes.INT())
                        .column("amount", DataTypes.INT())
                        .watermark("ts", "ts - INTERVAL '1' SECOND")
                        .build());

        // perform interval join
        Table joinedTable =
                tableEnv.sqlQuery(
                        "SELECT U.name, O.amount " +
                                "FROM UserTable U, OrderTable O " +
                                "WHERE U.uid = O.uid AND O.ts BETWEEN U.ts AND U.ts + INTERVAL '5' MINUTES");

        DataStream<Row> joinedStream = tableEnv.toDataStream(joinedTable);

        joinedStream.print();

        // implement a custom operator using ProcessFunction and value state
        joinedStream
                .keyBy(r -> r.<String>getFieldAs("name"))
                .process(
                        new KeyedProcessFunction<String, Row, String>() {

                            ValueState<String> seen;

                            @Override
                            public void open(Configuration parameters) {
                                seen = getRuntimeContext().getState(
                                        new ValueStateDescriptor<>("seen", String.class));
                            }

                            @Override
                            public void processElement(Row row, Context ctx, Collector<String> out)
                                    throws Exception {
                                String name = row.getFieldAs("name");
                                if (seen.value() == null) {
                                    seen.update(name);
                                    out.collect(name);
                                }
                            }
                        })
                .print();

        env.execute("test for two stream join");
    }
}
