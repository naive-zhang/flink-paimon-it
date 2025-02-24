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

    @Test
    public void testQueryWithHiveCatalog() {
        tableEnv.executeSql("create database if not exists myhive.test");
        tableEnv.executeSql("drop table if exists myhive.test.user_profile");
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS myhive.test.user_profile (\n" +
                " id INT NOT NULL,\n" +
                " name STRING,\n" +
                " age int,\n" +
                " gender STRING,\n" +
                " birthday date,\n" +
                " balance decimal(10, 2),\n" +
                " address string,\n" +
                " details string,\n" +
                " created_at TIMESTAMP,\n" +
                " updated_at TIMESTAMP,\n" +
                " last_login TIMESTAMP,\n" +
                " PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'scan.startup.mode' = 'earliest-offset',\n" +
                " 'server-time-zone' = 'Asia/Shanghai',\n" +
//                " 'server-time-zone' = 'GMT+08:00',\n" +
                " 'server-id' = '5400-5404',\n" +
                " 'hostname' = '" + mappedHost + "',\n" +
                " 'port' = '" + mappedPort + "',\n" +
                " 'username' = '" + JDBC_USER + "',\n" +
                " 'password' = '" + JDBC_PASS + "',\n" +
                " 'database-name' = 'inventory',\n" +
                " 'table-name' = 'user_profile'\n" +
                ")").print();
        tableEnv.sqlQuery("select * from myhive.test.user_profile")
                .execute()
                .print();
    }
}
