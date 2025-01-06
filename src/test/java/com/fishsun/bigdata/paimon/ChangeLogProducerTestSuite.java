package com.fishsun.bigdata.paimon;

import com.fishsun.bigdata.PaimonBasicTestSuite;
import com.fishsun.bigdata.PaimonChangeLogTestSuite;
import com.fishsun.bigdata.utils.ChangeLogUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.data.RowData;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ChangeLogProducerTestSuite extends PaimonChangeLogTestSuite {
    @Before
    public void setup() {
        super.setUp();
    }

    @Test
    public void testNoneModeChangeLogProducer() throws Exception {

    }

    @Test
    public void testInputModeChangeLogProducer() throws Exception {

    }

    @Test
    public void testLookupModeChangeLogProducer() throws Exception {

    }

    @Test
    public void testFullCompactionChangeLogProducer() throws Exception {

    }
}
