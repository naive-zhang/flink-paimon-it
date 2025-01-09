package com.fishsun.bigdata.paimon.merge;


import com.fishsun.bigdata.PaimonBasicTestSuite;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class PartialUpdateTestSuite extends PaimonBasicTestSuite {
    @Before
    @Override
    public void setUp() {
        super.setUp();
        try {
            tableEnv.executeSql("CREATE TABLE if not exists paimon.test.partial_update_tbl1(\n" +
                            " `id` Int,\n" +
                            "  `name` String,\n" +
                            "  `salary` BIGINT,\n" +
                            "   PRIMARY KEY (id) NOT ENFORCED\n" +
                            ") with  (\n" +
                            "'merge-engine' = 'partial-update',\n" +
                            "   'changelog-producer' = 'lookup'\n" +
                            ")")
                    .await();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        try {
            tableEnv.executeSql("CREATE TABLE if not exists paimon.test.partial_update_sequence_group_tbl(\n" +
                            " `id` Int,\n" +
                            "  `name` String,\n" +
                            "   `sg_1` Int,\n" +
                            "  `salary` BIGINT,\n" +
                            "   `sg_2` Int,\n" +
                            "   PRIMARY KEY (id) NOT ENFORCED\n" +
                            ") with  (\n" +
                            "'merge-engine' = 'partial-update',\n" +
                            "'changelog-producer' = 'input',\n" +
                            "'fields.sg_1.sequence-group' = 'name',\n" +
                            "'fields.sg_2.sequence-group' = 'salary'\n" +
                            ")")
                    .await();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void testPartialUpdate() throws ExecutionException, InterruptedException {
        tableEnv.executeSql("insert into paimon.test.partial_update_tbl1 values(1,'flink',CAST(NULL AS INT))")
                .await();
        tableEnv.executeSql("insert into paimon.test.partial_update_tbl1 values(1,cast(NULL as STRING),2000)")
                .await();
        tableEnv.sqlQuery("select * from paimon.test.partial_update_tbl1")
                .execute()
                .print();
    }

    @Test
    public void testQueryChangeLogFromPartialUpdate() {
        tableEnv.sqlQuery("select * from paimon.test.partial_update_tbl1 /*+ OPTIONS('scan.snapshot-id' = '1') */")
                .execute()
                .print();
    }

    @Test
    public void testSequenceGroupPartialUpdate() throws ExecutionException, InterruptedException {
        // partial_update_sequence_group_tbl
        tableEnv.executeSql("insert into paimon.test.partial_update_sequence_group_tbl values(1,'flink',1,1,1)").await();
        // --    output: +I | 1 | flink | 1 | 1 | 1
        tableEnv.executeSql("insert into paimon.test.partial_update_sequence_group_tbl values(1,'flink1',0,1,cast(NULL as Int))").await();
        // --    output: +I | 1 | flink | 1 | 1 | 1
        tableEnv.executeSql("insert into paimon.test.partial_update_sequence_group_tbl values(1,'flink2',1,2000,1)").await();
        // --    output: +I | 1 | flink2 | 1 | 2000 | 1
        tableEnv.executeSql("insert into paimon.test.partial_update_sequence_group_tbl values(1,'flink3',0,3000,0)").await();
        // --    output: +I | 1 | flink2 | 1 | 2000 | 1
        tableEnv.executeSql("insert into paimon.test.partial_update_sequence_group_tbl values(1,'flink3',2,3000,2)").await();
        // --    output: +I | 1 | flink3 | 2 | 3000 | 2
        tableEnv.sqlQuery("select * from paimon.test.partial_update_sequence_group_tbl")
                .execute()
                .print();
    }

    @Test
    public void testQueryChangeLogSequenceGroupPartialUpdate2() {
        tableEnv.sqlQuery("select * from paimon.test.partial_update_sequence_group_tbl /*+ OPTIONS('scan.snapshot-id' = '1') */")
                .execute()
                .print();
    }


}
