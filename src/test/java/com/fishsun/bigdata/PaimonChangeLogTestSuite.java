package com.fishsun.bigdata;

import com.fishsun.bigdata.utils.ChangeLogUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

public class PaimonChangeLogTestSuite extends PaimonBasicTestSuite {
    Table incomeInfoTbl;

    @Before
    public void setup() {
        super.setUp();
        List<Row> rowData = ChangeLogUtils.generateChangeLog(10);
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
    public void testGenerateChangeLogStream() throws Exception {
        env.fromCollection(ChangeLogUtils.generateChangeLog(10))
                .print();
        env.execute("test change log stream generation");
    }

    @Test
    public void testGenerateTblFromChangeLogStream() {
        tableEnv.sqlQuery("select * from income_info")
                .execute()
                .print();
    }
}
