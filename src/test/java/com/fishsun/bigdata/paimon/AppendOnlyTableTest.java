package com.fishsun.bigdata.paimon;

import com.fishsun.bigdata.PaimonBasicTestSuite;
import org.junit.Test;

public class AppendOnlyTableTest extends PaimonBasicTestSuite {
    @Test
    public void testScaleTable() {
        tableEnv.executeSql("CREATE TABLE if not exists paimon.test.scale_tbl(\n" +
                "  id bigint,\n" +
                "  name String,\n" +
                "  age Int,\n" +
                "  dt string\n" +
                ") PARTITIONED BY (dt) with  (\n" +
                " 'bucket' = '-1'\n" +
                ")");
        tableEnv.executeSql("insert into paimon.test.scale_tbl values\n" +
                "(1,'zhangsan',18,'2023-01-01'),\n" +
                "(1,'zhangsan',18,'2023-01-02'),\n" +
                "(1,'zhangsan',18,'2023-01-03');");
        tableEnv.sqlQuery("select * from paimon.test.scale_tbl")
                .execute()
                .print();
    }

    @Test
    public void testQueueTable() {
        tableEnv.executeSql("CREATE TABLE if not exists paimon.test.queue_tbl (\n" +
                "  id bigint,\n" +
                "  name String,\n" +
                "  age Int,\n" +
                "  dt string\n" +
                ")  with  (\n" +
                " 'bucket' = '5',\n" +
                " 'bucket-key' = 'id'\n" +
                ")");
        tableEnv.executeSql("insert into paimon.test.queue_tbl values\n" +
                "(1,'zhangsan',18,'2023-01-01'),\n" +
                "(2,'zhangsan',18,'2023-01-01'),\n" +
                "(3,'zhangsan',18,'2023-01-02'),\n" +
                "(3,'zhangsan',18,'2023-01-02'),\n" +
                "(4,'zhangsan',18,'2023-01-02'),\n" +
                "(5,'zhangsan',18,'2023-01-02'),\n" +
                "(6,'zhangsan',18,'2023-01-02'),\n" +
                "(7,'zhangsan',18,'2023-01-02'),\n" +
                "(8,'zhangsan',18,'2023-01-03')");
        tableEnv.sqlQuery("select * from paimon.test.queue_tbl")
                .execute()
                .print();
    }
}
