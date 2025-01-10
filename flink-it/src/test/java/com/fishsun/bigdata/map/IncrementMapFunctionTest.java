package com.fishsun.bigdata.map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IncrementMapFunctionTest {
    @Test
    public void testIncrementMapFunction() throws Exception {
        IncrementMapFunction incrementMapFunction = new IncrementMapFunction();
        assertEquals((Long) 3L, incrementMapFunction.map(2L));
    }
}
