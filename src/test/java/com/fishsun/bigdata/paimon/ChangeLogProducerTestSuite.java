package com.fishsun.bigdata.paimon;

import com.fishsun.bigdata.PaimonBasicTestSuite;
import com.fishsun.bigdata.utils.ChangeLogUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

public class ChangeLogProducerTestSuite extends PaimonBasicTestSuite {
    Table incomeInfoTbl;

    @Before
    public void setup() {
        super.setUp();
        List<Row> rowData = ChangeLogUtils.generateChangeLog(100);
        DataStreamSource<Row> incomeInfoStream = env.fromCollection(rowData);
        incomeInfoTbl = tableEnv.fromChangelogStream(incomeInfoStream
        ).renameColumns(
                $("f0").as("name"),
                $("f1").as("gender"),
                $("f2").as("dept"),
                $("f3").as("income")
        );
        tableEnv.createTemporaryView("default_catalog.test.income_info", incomeInfoTbl);
    }

    @Test
    public void testNoneModeChangeLogProducer() throws Exception {
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
        tableEnv.sqlQuery("select name, gender, dept, income from paimon.test.income_info_none")
                .execute()
                .print();
    }

    @Test
    public void testInputModeChangeLogProducer() throws Exception {

    }

    @Test
    public void testLookupModeChangeLogProducer() throws Exception {

    }

    @Test
    public void testFullCompactionChangeLogProducer() throws Exception {

    }
}
