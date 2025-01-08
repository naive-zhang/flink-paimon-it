package com.fishsun.bigdata.paimon;

import com.fishsun.bigdata.PaimonChangeLogTestSuite;
import com.fishsun.bigdata.utils.FileUtils;
import org.junit.Before;
import org.junit.Test;


public class ChangeLogProducerTestSuite extends PaimonChangeLogTestSuite {

    @Before
    @Override
    public void setUp() {
        super.setUp();
    }

    @Test
    public void testNoneModeChangeLogProducer() throws Exception {
        FileUtils.clearDir(FileUtils.getLakehouseDefaultPath(false) + "./test.db/income_info_none");
        tableEnv.executeSql("drop table if exists paimon.test.income_info_none");
        tableEnv.executeSql("CREATE TABLE if not exists paimon.test.income_info_none(\n" +
                " `name` String,\n" +
                " `gender` String,\n" +
                " `dept` String,\n" +
                " `income` Double,\n" +
                " PRIMARY KEY (name) NOT ENFORCED\n" +
                ") with  (\n" +
                "'merge-engine' = 'deduplicate',\n" +
                "'bucket' = '1',\n" +
                "'changelog-producer'='none'\n" +
                ");");
        tableEnv.executeSql("insert into paimon.test.income_info_none \n" +
                        "select name, gender, dept, income from default_catalog.test.income_info")
                .print();
        tableEnv.sqlQuery("select name, gender, dept, income from paimon.test.income_info_none " +
                        "/*+ OPTIONS('scan.snapshot-id' = '1') */")
                .execute()
                .print();
    }

    @Test
    public void testQueryFromNodeModeChangeLogProducer() throws Exception {
        tableEnv.sqlQuery("select name, gender, dept, income from paimon.test.income_info_none ")
                .execute()
                .print();
    }

    @Test
    public void testInputModeChangeLogProducer() throws Exception {
        FileUtils.clearDir(FileUtils.getLakehouseDefaultPath(false) +
                "./test.db/income_info_input");
        tableEnv.executeSql("drop table if exists paimon.test.income_info_input");
        tableEnv.executeSql("CREATE TABLE if not exists paimon.test.income_info_input(\n" +
                " `name` String,\n" +
                " `gender` String,\n" +
                " `dept` String,\n" +
                " `income` Double,\n" +
                " PRIMARY KEY (name) NOT ENFORCED\n" +
                ") with  (\n" +
                "'merge-engine' = 'deduplicate',\n" +
                "'bucket' = '1',\n" +
                "'changelog-producer'='input'\n" +
                ");");
        tableEnv.executeSql("insert into paimon.test.income_info_input \n" +
                        "select name, gender, dept, income from default_catalog.test.income_info")
                .print();
        tableEnv.sqlQuery("select name, gender, dept, income from paimon.test.income_info_input " +
                        "/*+ OPTIONS('scan.snapshot-id' = '1') */")
                .execute()
                .print();
    }

    @Test
    public void testQueryFromInputModeChangeLogProducer() throws Exception {
        tableEnv.sqlQuery("select name, gender, dept, income from " +
                        "paimon.test.income_info_input ")
                .execute()
                .print();
    }

    @Test
    public void testLookupModeChangeLogProducer() throws Exception {
        FileUtils.clearDir(FileUtils.getLakehouseDefaultPath(false) +
                "./test.db/income_info_lookup_agg");
        tableEnv.executeSql("drop table if exists paimon.test.income_info_lookup_agg");
        tableEnv.executeSql("CREATE TABLE if not exists paimon.test.income_info_lookup_agg(\n" +
                " `name` String,\n" +
                " `sum_income` Double,\n" +
                "  `change_cnt` Int,\n" +
                " PRIMARY KEY (name) NOT ENFORCED\n" +
                ") with  (\n" +
                "'merge-engine' = 'aggregation',\n" +
                "'field.sum_income.aggregate-function' = 'sum',\n" +
                "'field.change_cnt.aggregate-function' = 'sum',\n" +
                "'bucket' = '1',\n" +
                "'changelog-producer'='lookup'\n" +
                ");");
        tableEnv.executeSql("insert into paimon.test.income_info_lookup_agg \n" +
                        "select name" +
                        ", income " +
                        ", 1" +
                        "from default_catalog.test.income_info")
                .print();
        tableEnv.sqlQuery("select name" +
                        ", sum_income" +
                        ", change_cnt " +
                        "from paimon.test.income_info_lookup_agg " +
                        "/*+ OPTIONS('scan.snapshot-id' = '1') */")
                .execute()
                .print();
    }

    @Test
    public void testQueryIncomeInfoInputAgg() {
        tableEnv.sqlQuery(
                        "select * from paimon.test.income_info_lookup_agg"
                )
                .execute()
                .print();
    }

    @Test
    public void testFullCompactionChangeLogProducer() throws Exception {
        FileUtils.clearDir(FileUtils.getLakehouseDefaultPath(false) +
                "./test.db/income_info_full_compaction_agg");
        tableEnv.executeSql("drop table if exists paimon.test.income_info_full_compaction_agg");
        tableEnv.executeSql("CREATE TABLE if not exists paimon.test.income_info_full_compaction_agg(\n" +
                " `name` String,\n" +
                " `sum_income` Double,\n" +
                "  `change_cnt` Int,\n" +
                " PRIMARY KEY (name) NOT ENFORCED\n" +
                ") with  (\n" +
                "'merge-engine' = 'aggregation',\n" +
                "'field.sum_income.aggregate-function' = 'sum',\n" +
                "'field.change_cnt.aggregate-function' = 'sum',\n" +
                "'bucket' = '1',\n" +
                "'changelog-producer'='full-compaction',\n" +
                "'full-compaction.delta-commits'='3'\n" +
                ");");
        tableEnv.executeSql("insert into paimon.test.income_info_full_compaction_agg \n" +
                        "select name" +
                        ", income " +
                        ", 1" +
                        "from default_catalog.test.income_info")
                .print();
        tableEnv.sqlQuery("select name" +
                        ", sum_income" +
                        ", change_cnt " +
                        "from paimon.test.income_info_full_compaction_agg " +
                        "/*+ OPTIONS('scan.snapshot-id' = '1') */")
                .execute()
                .print();
    }
}
