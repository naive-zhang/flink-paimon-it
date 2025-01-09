package com.fishsun.bigdata.paimon.merge;

import com.fishsun.bigdata.PaimonChangeLogTestSuite;
import com.fishsun.bigdata.utils.FileUtils;
import org.junit.Before;
import org.junit.Test;

public class AggregationEngineTestSuite extends PaimonChangeLogTestSuite {
    @Before
    @Override
    public void setUp() {
        super.setUp();
        // FileUtils.clearDir(FileUtils.getLakehouseDefaultPath(false)
        //         + "/test.db/agg_tbl_input");
        // FileUtils.clearDir(FileUtils.getLakehouseDefaultPath(false)
        //         + "/test.db/agg_tbl_lookup");
        // FileUtils.clearDir(FileUtils.getLakehouseDefaultPath(false)
        //         + "/test.db/agg_tbl_full_compaction");
        tableEnv.executeSql("CREATE TABLE if not exists paimon.test.agg_tbl_input(" +
                "`id` Int,\n" +
                "  `name` String,\n" +
                "  `salary` Int,\n" +
                "  `sum_cnt` Int,\n" +
                "   PRIMARY KEY (id) NOT ENFORCED\n" +
                ")  with  (\n" +
                "  'merge-engine' = 'aggregation',\n" +
                "  'fields.salary.aggregate-function' = 'max',\n" +
                "  'fields.sum_cnt.aggregate-function' = 'sum',\n" +
                "   'changelog-producer' = 'input'\n" +
                ");");

        tableEnv.executeSql("CREATE TABLE if not exists paimon.test.agg_tbl_lookup(" +
                "`id` Int,\n" +
                "  `name` String,\n" +
                "  `salary` Int,\n" +
                "  `sum_cnt` Int,\n" +
                "   PRIMARY KEY (id) NOT ENFORCED\n" +
                ")  with  (\n" +
                "  'merge-engine' = 'aggregation',\n" +
                "  'fields.salary.aggregate-function' = 'max',\n" +
                "  'fields.sum_cnt.aggregate-function' = 'sum',\n" +
                "   'changelog-producer' = 'lookup'\n" +
                ");");

        tableEnv.executeSql("CREATE TABLE if not exists paimon.test.agg_tbl_full_compaction(" +
                "`id` Int,\n" +
                "  `name` String,\n" +
                "  `salary` Int,\n" +
                "  `sum_cnt` Int,\n" +
                "   PRIMARY KEY (id) NOT ENFORCED\n" +
                ")  with  (\n" +
                "  'merge-engine' = 'aggregation',\n" +
                "  'fields.salary.aggregate-function' = 'max',\n" +
                "  'fields.sum_cnt.aggregate-function' = 'sum',\n" +
                "   'changelog-producer' = 'full-compaction'\n" +
                ");");
    }

    @Test
    public void testInsertData() throws InterruptedException {
        tableEnv.executeSql("insert into paimon.test.agg_tbl_input values(2,'flink',1000,1000)")
                .print();
        tableEnv.executeSql("insert into paimon.test.agg_tbl_lookup values(2,'flink',1000,1000)")
                .print();
        tableEnv.executeSql("insert into paimon.test.agg_tbl_full_compaction values(2,'flink',1000,1000)")
                .print();
        Thread.sleep(10000);
        tableEnv.executeSql("insert into paimon.test.agg_tbl_input values(2,'flink',2000,500)")
                .print();
        tableEnv.executeSql("insert into paimon.test.agg_tbl_lookup values(2,'flink',2000,500)")
                .print();
        tableEnv.executeSql("insert into paimon.test.agg_tbl_full_compaction values(2,'flink',2000,500)")
                .print();
        Thread.sleep(10000);
        tableEnv.executeSql("insert into paimon.test.agg_tbl_input  values(3,'flink',500,500)")
                .print();
        tableEnv.executeSql("insert into paimon.test.agg_tbl_lookup  values(3,'flink',500,500)")
                .print();
        tableEnv.executeSql("insert into paimon.test.agg_tbl_full_compaction  values(3,'flink',500,500)")
                .print();
        // tableEnv.sqlQuery("select * from paimon.test.agg_tbl_input");
    }

    @Test
    public void testQueryFromAggTblWithInput() {
        tableEnv.sqlQuery("select * from paimon.test.agg_tbl_input")
                .execute()
                .print();
    }

    @Test
    public void testQueryFromAggTblWithLookup() {
        tableEnv.sqlQuery("select * from paimon.test.agg_tbl_lookup")
                .execute()
                .print();
    }

    @Test
    public void testQueryFromAggTblWithFullCompaction() {
        tableEnv.sqlQuery("select * from paimon.test.agg_tbl_full_compaction")
                .execute()
                .print();
    }

    @Test
    public void testQueryFromAggTblWithInputAndHint() {
        tableEnv.sqlQuery("select * from paimon.test.agg_tbl_input /*+ OPTIONS('scan.snapshot-id' = '1') */")
                .execute()
                .print();
    }

    @Test
    public void testQueryFromAggTblWithLookupAndHint() {
        tableEnv.sqlQuery("select * from paimon.test.agg_tbl_lookup /*+ OPTIONS('scan.snapshot-id' = '1') */")
                .execute()
                .print();
    }


    @Test
    public void testQueryFromAggTblWithFullCompactionAndHint() {
        tableEnv.sqlQuery("select * from paimon.test.agg_tbl_full_compaction /*+ OPTIONS('scan.snapshot-id' = '1') */")
                .execute()
                .print();
    }

}
