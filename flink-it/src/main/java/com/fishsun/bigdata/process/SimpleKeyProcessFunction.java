package com.fishsun.bigdata.process;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class SimpleKeyProcessFunction extends KeyedProcessFunction<String, String, String> {
    @Override
    public void processElement(String value
            , KeyedProcessFunction<String, String, String>.Context context
            , Collector<String> collector) throws Exception {
        context.timerService().registerProcessingTimeTimer(50);
        collector.collect("vx=>" + value);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, String, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        out.collect(String.format("定时器在 %d 被触发", timestamp));
    }
}
