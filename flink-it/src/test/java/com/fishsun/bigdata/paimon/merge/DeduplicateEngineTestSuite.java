package com.fishsun.bigdata.paimon.merge;

import com.fishsun.bigdata.PaimonChangeLogTestSuite;
import com.fishsun.bigdata.utils.FileUtils;
import org.junit.Before;
import org.junit.Test;

public class DeduplicateEngineTestSuite extends PaimonChangeLogTestSuite {
    @Before
    @Override
    public void setUp() {
        super.setUp();
        FileUtils.clearDir(FileUtils.getLakehouseDefaultPath(false) +
                "/test.db/deduplicate_tbl");
        tableEnv.executeSql("CREATE TABLE if not exists paimon.test.deduplicate_tbl(\n" +
                " `id` Int,\n" +
                "  `name` String,\n" +
                "  `salary` Int,\n" +
                "   PRIMARY KEY (id) NOT ENFORCED\n" +
                ") with  (\n" +
                "'merge-engine' = 'deduplicate',\n" +
                "'changelog-producer' = 'input'\n" +
                ")");
    }

    @Test
    public void testInsertData() throws InterruptedException {
        tableEnv.executeSql("insert into paimon.test.deduplicate_tbl " +
                "values(1,'flink',1000)");
        Thread.sleep(10000);
        tableEnv.executeSql("insert into paimon.test.deduplicate_tbl " +
                "values(1,'flink',2000)");
        Thread.sleep(10000);
        tableEnv.executeSql("insert into paimon.test.deduplicate_tbl " +
                "values(1,'flink',500)");
//        tableEnv.sqlQuery("select * from paimon.test.deduplicate_tbl")
//                .execute()
//                .print();
        tableEnv.sqlQuery("select * from paimon.test.deduplicate_tbl /*+ OPTIONS('scan.snapshot-id' = '1') */")
                .execute()
                .print();
    }
}
