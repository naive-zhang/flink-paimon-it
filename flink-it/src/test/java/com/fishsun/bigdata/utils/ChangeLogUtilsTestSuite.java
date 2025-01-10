package com.fishsun.bigdata.utils;

import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.List;

import static com.fishsun.bigdata.utils.ChangeLogUtils.generateChangeLog;

public class ChangeLogUtilsTestSuite {
    @Test
    public void testChangeLogGenerate() {
        List<Row> changeLog = generateChangeLog(10);

        // 打印 ChangeLog 数据
        for (Row row : changeLog) {
            System.out.println(row.toString());
        }
    }
}
