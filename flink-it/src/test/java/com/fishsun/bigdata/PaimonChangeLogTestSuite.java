package com.fishsun.bigdata;

import com.fishsun.bigdata.source.RowSourceFunction;
import com.fishsun.bigdata.utils.ChangeLogUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

public class PaimonChangeLogTestSuite extends PaimonBasicTestSuite {
    DataStream<Row> incomeInfoStream;
    Table incomeInfoTbl;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        List<Row> genericRowDataList = ChangeLogUtils.generateChangeLog(10);
        // 定义 RowTypeInfo，明确指定每个字段的类型
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                Types.STRING(),  // Field 0: name
                Types.STRING(),  // Field 1: gender
                Types.STRING(),  // Field 2: department
                Types.DOUBLE()   // Field 3: salary
        );
        RowSourceFunction rowSourceFunction = new RowSourceFunction(genericRowDataList, checkpointInterval);
        incomeInfoStream = env.addSource(rowSourceFunction)
                .returns(rowTypeInfo);
        incomeInfoTbl = tableEnv.fromChangelogStream(incomeInfoStream,
                        Schema.newBuilder()
                                .column("f0", DataTypes.STRING())
                                .column("f1", DataTypes.STRING())
                                .column("f2", DataTypes.STRING())
                                .column("f3", DataTypes.DOUBLE())
                                .build())
                .renameColumns($("f0").as("name"),
                        $("f1").as("gender"),
                        $("f2").as("dept"),
                        $("f3").as("income"));
        tableEnv.createTemporaryView("default_catalog.test.income_info", incomeInfoTbl);
    }

    @Test
    public void testGenerateChangeLogStream() throws Exception {
        incomeInfoStream
                .print();
        env.execute("test change log stream generation");
    }

    @Test
    public void testGenerateTblFromChangeLogStream() {
        incomeInfoTbl.execute()
                .print();
    }

}
