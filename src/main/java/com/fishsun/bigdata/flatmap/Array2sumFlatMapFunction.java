package com.fishsun.bigdata.flatmap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class Array2sumFlatMapFunction implements FlatMapFunction<String, Integer> {
    @Override
    public void flatMap(String s, Collector<Integer> collector) throws Exception {
        int sum = 0;
        for (String strNum : s.split(",")) {
            sum += Integer.parseInt(strNum.trim());
        }
        collector.collect(sum);
    }
}
