package com.fishsun.bigdata.flatmap;


import org.apache.flink.util.Collector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.mock;


@RunWith(MockitoJUnitRunner.class)
public class Array2sumFlatMapFunctionTest {

    @Test
    public void testSum() throws Exception {
        Array2sumFlatMapFunction array2sumFlatMapFunction = new Array2sumFlatMapFunction();
        Collector<Integer> collector = mock(Collector.class);
        array2sumFlatMapFunction.flatMap("1,2,3,4,5", collector);
        Mockito.verify(collector, Mockito.times(1)).collect(15);
    }
}
