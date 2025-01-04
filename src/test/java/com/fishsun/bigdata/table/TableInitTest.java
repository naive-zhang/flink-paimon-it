package com.fishsun.bigdata.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Before;
import org.junit.Test;

public class TableInitTest {
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tableEnv;

    @Before
    public void setUp() {
        // 创建 Flink 流执行环境和 SQL 表环境
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tableEnv = StreamTableEnvironment.create(env);
    }

    @Test
    public void testSimpleSqlQuery() throws Exception {
        // Create a source table
        tableEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
                .build());

        // Create a sink table (using SQL DDL)
        tableEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') " +
                "LIKE SourceTable (EXCLUDING OPTIONS) ");

        // Create a Table object from a Table API query
        Table table1 = tableEnv.from("SourceTable");

        // Create a Table object from a SQL query
        Table table2 = tableEnv.sqlQuery("SELECT * FROM SourceTable");

        // Emit a Table API result Table to a TableSink, same for SQL result
        // TableResult tableResult = table1.insertInto("SinkTable").execute();
        tableEnv.executeSql("select * from SourceTable")
                .print();
    }
}
