package com.fishsun.bigdata.process;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.Lists;

public class SimpleKeyProcessFunctionTest {
    private OneInputStreamOperatorTestHarness<String, String> testHarness;

    @Before
    public void setUp() throws Exception {
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(
                        new SimpleKeyProcessFunction()
                ),
                x -> "1",
                Types.STRING
        );
        testHarness.open();
    }

    @Test
    public void testProcess() throws Exception {
        testHarness.processElement("9527", 10);
        Assert.assertEquals(Lists.newArrayList(
                        new StreamRecord<>("vx=>9527", 10)
                ),
                testHarness.extractOutputStreamRecords()
        );

        Assert.assertEquals(Lists.newArrayList(
                        "vx=>9527"
                ),
                testHarness.extractOutputValues()
        );
    }

    @Test
    public void testProcessWithTimer() throws Exception {
        // test first record
        testHarness.processElement("alanchanchn", 80);
        Assert.assertEquals(1, testHarness.numProcessingTimeTimers());
        // Function time 设置为 100
        testHarness.setProcessingTime(100);
        Assert.assertEquals(
                Lists.newArrayList(
                        new StreamRecord<>("vx=>alanchanchn", 80),
                        new StreamRecord<>("定时器在 50 被触发")),
                testHarness.extractOutputStreamRecords());
        testHarness.processElement("bob", 100);
        testHarness.setProcessingTime(200);
        Assert.assertEquals(
                Lists.newArrayList(
                        new StreamRecord<>("vx=>alanchanchn", 80),
                        new StreamRecord<>("定时器在 50 被触发"),
                        new StreamRecord<>("vx=>bob", 100),
                        new StreamRecord<>("定时器在 50 被触发")
                ),
                testHarness.extractOutputStreamRecords());
    }
}
