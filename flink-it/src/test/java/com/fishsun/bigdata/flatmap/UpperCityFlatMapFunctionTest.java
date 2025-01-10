package com.fishsun.bigdata.flatmap;

import com.fishsun.bigdata.model.Person;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.Lists;

public class UpperCityFlatMapFunctionTest {
    OneInputStreamOperatorTestHarness<Person, Person> testHarness;
    UpperCityFlatMapFunction flatMapFunction;

    @Before
    public void setUp() throws Exception {
        flatMapFunction = new UpperCityFlatMapFunction();
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(flatMapFunction),
                new KeySelector<Person, String>() {
                    @Override
                    public String getKey(Person person) throws Exception {
                        return person.getCity();
                    }
                }, Types.STRING);
        testHarness.open();
    }

    @Test
    public void testFlatMap() throws Exception {
        testHarness.processElement(new Person(1, "alanchan", 18, "sh"), 10);
        ValueState<Person> previousInput = flatMapFunction.getRuntimeContext().getState(
                new ValueStateDescriptor<>("previous-person", Person.class));
        Person stateValue = previousInput.value();
        Assert.assertEquals(
                Lists.newArrayList(new StreamRecord<>(new Person(1, "alanchan", 18, "sh".toUpperCase()), 10)),
                testHarness.extractOutputStreamRecords());
        Assert.assertEquals(new Person(1, "alanchan", 18, "sh".toUpperCase()), stateValue);
        testHarness.processElement(new Person(2, "alan", 19, "bj"), 10000);
        Assert.assertEquals(
                Lists.newArrayList(
                        new StreamRecord<>(new Person(1, "alanchan", 18, "sh".toUpperCase()), 10),
                        new StreamRecord<>(new Person(2, "alan", 19, "bj".toUpperCase()), 10000)),
                testHarness.extractOutputStreamRecords());
        Assert.assertEquals(new Person(2, "alan", 19, "bj".toUpperCase()), previousInput.value());
    }
}
