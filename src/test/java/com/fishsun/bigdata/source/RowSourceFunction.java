package com.fishsun.bigdata.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class RowSourceFunction implements SourceFunction<Row>, Serializable {


    List<Row> rowList;
    long checkpointInterval;

    public RowSourceFunction(List<Row> rowList, long checkpointInterval) {
        this.rowList = rowList;
        this.checkpointInterval = checkpointInterval;
    }

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {
        System.out.println("----------------- row source function is running -----------------");
        List<Row> insertRows = new ArrayList<>();
        List<Row> updateBeforeRows = new ArrayList<>();
        List<Row> updateAfterRows = new ArrayList<>();
        List<Row> deleteRows = new ArrayList<>();

        for (Row row : rowList) {
            if (row.getKind() == RowKind.INSERT) {
                insertRows.add(row);
            } else if (row.getKind() == RowKind.UPDATE_BEFORE) {
                updateBeforeRows.add(row);
            } else if (row.getKind() == RowKind.UPDATE_AFTER) {
                updateAfterRows.add(row);
            } else {
                deleteRows.add(row);
            }
        }
        for (int i = 0; i < insertRows.size(); i++) {
            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(insertRows.get(i));
            }
        }
        Thread.sleep(checkpointInterval + 5000);
        for (int i = 0; i < updateBeforeRows.size(); i++) {
            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(updateBeforeRows.get(i));
                sourceContext.collect(updateAfterRows.get(i));
            }
        }
        Thread.sleep(checkpointInterval + 5000);
        for (int i = 0; i < deleteRows.size(); i++) {
            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(deleteRows.get(i));
            }
        }
        cancel();
    }

    @Override
    public void cancel() {
    }
}
