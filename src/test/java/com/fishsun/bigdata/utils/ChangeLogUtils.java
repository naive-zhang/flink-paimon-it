package com.fishsun.bigdata.utils;

import com.fishsun.bigdata.model.IncomeInfo;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ChangeLogUtils {

    private static final Random RANDOM = new Random();

    // 构造单个 RowData
    private static RowData createRow(IncomeInfo incomeInfo, RowKind rowKind) {
        GenericRowData row = new GenericRowData(rowKind, 4);
        row.setField(0, incomeInfo.name);
        row.setField(1, incomeInfo.gender);
        row.setField(2, incomeInfo.dept);
        row.setField(3, incomeInfo.income);
        return row;
    }

    // 生成随机 IncomeInfo 对象
    private static IncomeInfo randomIncomeInfo() {
        IncomeInfo info = new IncomeInfo();
        info.name = IncomeInfo.pickupName();
        info.gender = IncomeInfo.pickupGender();
        info.dept = IncomeInfo.pickupDept();
        info.income = Math.ceil(3000 + RANDOM.nextDouble() * 10000); // 随机生成收入
        return info;
    }

    // 生成 ChangeLog 数据流
    public static List<RowData> generateChangeLog(int recordCount) {
        List<RowData> changeLog = new ArrayList<>();

        for (int i = 0; i < recordCount; i++) {
            IncomeInfo baseInfo = randomIncomeInfo();

            // 模拟 INSERT 操作
            changeLog.add(createRow(baseInfo, RowKind.INSERT));

            // 模拟 UPDATE 操作
            if (RANDOM.nextBoolean()) {
                IncomeInfo updatedInfo = new IncomeInfo();
                updatedInfo.name = baseInfo.name;
                updatedInfo.gender = baseInfo.gender;
                updatedInfo.dept = IncomeInfo.pickupDept(); // 部门可能变更
                updatedInfo.income = baseInfo.income + 1000; // 收入增加

                changeLog.add(createRow(baseInfo, RowKind.UPDATE_BEFORE));
                changeLog.add(createRow(updatedInfo, RowKind.UPDATE_AFTER));
            }

            // 模拟 DELETE 操作
            if (RANDOM.nextBoolean()) {
                changeLog.add(createRow(baseInfo, RowKind.DELETE));
            }
        }

        return changeLog;
    }
}
