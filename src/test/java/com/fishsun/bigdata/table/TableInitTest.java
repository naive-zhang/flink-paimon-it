package com.fishsun.bigdata.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
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
        // 准备测试数据
        String createTableStmt = "CREATE TEMPORARY TABLE source_table (" +
                "id BIGINT, " +
                "name STRING" +
                ") WITH ('connector' = 'print', 'bounded' = 'true', " +
                "'data' = '[(1, \"Alice\"), (2, \"Bob\"), (3, \"Charlie\")]')";
        tableEnv.executeSql(createTableStmt);

        // 执行 SQL 查询
        String query = "SELECT * FROM source_table WHERE id > 1";
        TableResult result = tableEnv.executeSql(query);

        // 获取查询结果并验证
        result.print();

        // 你可以使用 Table API 来进行更多的验证操作
        // 此处假设你期望的输出是 [(2, "Bob"), (3, "Charlie")]
        Table expectedOutput = tableEnv.fromDataStream(env.fromElements(
                Tuple2.of(1, "Alice"),
                Tuple2.of(2L, "Bob"),
                Tuple2.of(3L, "Charlie")
        ));

        // 执行 Table 查询并检查输出结果
        tableEnv.executeSql(query).print();
    }
}
