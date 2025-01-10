package com.fishsun.bigdata.flatmap;


import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedDeque;

public class OvenEvenFlatMapFunctionTest {
    OneInputStreamOperatorTestHarness<Integer, Integer> testHarness;

    @Before
    public void setUpTestHarness() throws Exception {
        StreamFlatMap<Integer, Integer> operator = new StreamFlatMap<>(new OvenEvenFlatMapFunction());
        testHarness =
                new OneInputStreamOperatorTestHarness<Integer, Integer>(operator);
        testHarness.open();
    }

    @Test
    public void testFlatMap() throws Exception {
        long initialTime = 0L;
        ConcurrentLinkedDeque<Object> expectedOutput = new ConcurrentLinkedDeque<>();
        testHarness.processElement(new StreamRecord<Integer>(1, initialTime + 1));
        testHarness.processElement(new StreamRecord<Integer>(2, initialTime + 2));
        testHarness.processWatermark(new Watermark(initialTime + 2));
        testHarness.processElement(new StreamRecord<Integer>(3, initialTime + 3));
        testHarness.processElement(new StreamRecord<Integer>(4, initialTime + 4));
        testHarness.processElement(new StreamRecord<Integer>(5, initialTime + 5));
        testHarness.processElement(new StreamRecord<Integer>(6, initialTime + 6));
        testHarness.processElement(new StreamRecord<Integer>(7, initialTime + 7));
        testHarness.processElement(new StreamRecord<Integer>(8, initialTime + 8));
        expectedOutput.add(new StreamRecord<Integer>(2, initialTime + 2));
        expectedOutput.add(new StreamRecord<Integer>(4, initialTime + 2));
        expectedOutput.add(new Watermark(initialTime + 2));
        expectedOutput.add(new StreamRecord<Integer>(4, initialTime + 4));
        expectedOutput.add(new StreamRecord<Integer>(16, initialTime + 4));
        expectedOutput.add(new StreamRecord<Integer>(6, initialTime + 6));
        expectedOutput.add(new StreamRecord<Integer>(36, initialTime + 6));
        expectedOutput.add(new StreamRecord<Integer>(8, initialTime + 8));
        expectedOutput.add(new StreamRecord<Integer>(64, initialTime + 8));
        TestHarnessUtil.assertOutputEquals("输出结果", expectedOutput, testHarness.getOutput());
    }
}
