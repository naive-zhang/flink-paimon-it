package com.fishsun.bigdata.source;

import com.fishsun.bigdata.model.IncomeInfo;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;
import org.testcontainers.shaded.org.checkerframework.checker.units.qual.A;

import java.util.ArrayList;
import java.util.List;

public class IncomeInfoSourceFunction extends RichSourceFunction<IncomeInfo> {

    int processCnt = 0;
    List<Row> incomeInfoList;

    public IncomeInfoSourceFunction(List<Row> incomeInfoList) {
        this.incomeInfoList = incomeInfoList;
    }

    @Override
    public void run(SourceContext<IncomeInfo> sourceContext) throws Exception {
        List<Row> insertRows = new ArrayList<Row>();
        List<Row> updateRows = new ArrayList<Row>();
        List<Row> deleteRows = new ArrayList<Row>();

        for (Row row : incomeInfoList) {
            // TODO 根据类型拆分数据
        }
    }

    @Override
    public void cancel() {

    }

    @Override
    public void close() throws Exception {

    }
}
