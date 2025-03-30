package com.fishsun.bigdata.cdc;

import org.junit.Test;

/**
 * @Author: zhangxinsen
 * @Date: 2025/3/29 00:16
 * @Desc:
 * @Version: v1.0
 */

public class MySQLCdcTestSuite extends BasicMySQLCdcTestSuite{
    /**
     * 测试 alter table 以及 alter 之后的 insert 操作
     */
    @Test
    @Override
    public void testInsertIntoPaimonTableFromCdc() {
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
                " 'server-id' = '5404-5408',\n" +
                " 'hostname' = '" + mappedHost + "',\n" +
                " 'port' = '" + mappedPort + "',\n" +
                " 'username' = '" + JDBC_USER + "',\n" +
                " 'password' = '" + JDBC_PASS + "',\n" +
                " 'database-name' = 'inventory',\n" +
                " 'table-name' = 'user_profile'\n" +
                ")").print();
        tableEnv.executeSql("alter table mypaimon.paimon_ods.ods_test_user_profile_rt add last_login_date date").print();
        tableEnv.executeSql("insert into mypaimon.paimon_ods.ods_test_user_profile_rt " +
                        "select id, name, age, gender, birthday, balance, address, details, created_at, updated_at, last_login, cast(created_at as date) as dt, 0 , 0, updated_at, cast(last_login as date)   from myhive.test.user_profile")
                .print();
    }
}
