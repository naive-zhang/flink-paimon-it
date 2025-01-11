package com.fishsun.bigdata.cdc;

import org.junit.Before;
import org.junit.Test;

public class BasicMySQLCdcTestSuite extends BasicMySQLCdcWithDockerComposeTestSuite {

    @Override
    protected void init() {
        mappedHost = "localhost";
        mappedPort = 3306;
    }

    @Override
    @Before
    public void setUp() {
        USING_INTERNAL_DOCKER_COMPOSE = false;
        super.setUp();
    }

    @Test
    @Override
    public void testFlinkCdcIntegration() throws Exception {
        super.testFlinkCdcIntegration();
    }

    @Test
    @Override
    public void testQueryFromCdcTable() {
        super.testQueryFromCdcTable();
    }
}
