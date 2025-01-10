package com.fishsun.bigdata.flatmap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class OvenEvenFlatMapFunction implements FlatMapFunction<Integer, Integer> {
    @Override
    public void flatMap(Integer integer, Collector<Integer> collector) throws Exception {
        if (integer % 2 == 0) {
            collector.collect(integer);
            collector.collect(integer * integer);
        }
    }
}
