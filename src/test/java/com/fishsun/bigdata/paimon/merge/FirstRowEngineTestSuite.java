package com.fishsun.bigdata.paimon.merge;

import com.fishsun.bigdata.PaimonChangeLogTestSuite;
import org.junit.Before;
import org.junit.Test;

public class FirstRowEngineTestSuite extends PaimonChangeLogTestSuite {
    @Before
    @Override
    public void setUp() {
        super.setUp();
        tableEnv.executeSql("CREATE TABLE if not exists paimon.test.first_row_tbl(\n" +
                " `id` Int,\n" +
                "  `name` String,\n" +
                "  `salary` Int,\n" +
                "   PRIMARY KEY (id) NOT ENFORCED\n" +
                ") with  (\n" +
                "'merge-engine' = 'first-row',\n" +
                "'changelog-producer'='lookup'\n" +
                " )");
    }

    @Test
    public void testInsertData() throws InterruptedException {
        tableEnv.executeSql("insert into paimon.test.first_row_tbl" +
                " values(1,'flink',1000)");
        Thread.sleep(10000);
        tableEnv.executeSql("insert into paimon.test.first_row_tbl" +
                " values(1,'flink',2000)");
        Thread.sleep(10000);
        tableEnv.executeSql("select * from paimon.test.first_row_tbl")
                .print();
    }

    @Test
    public void testChangeLogQueryFromFirstRowTbl() {
        tableEnv.executeSql(" SELECT * FROM paimon.test.first_row_tbl " +
                "/*+ OPTIONS('scan.snapshot-id' = '1') */")
                .print();
    }
}
