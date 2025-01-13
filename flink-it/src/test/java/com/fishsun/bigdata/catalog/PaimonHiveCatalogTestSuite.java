package com.fishsun.bigdata.catalog;

import com.fishsun.bigdata.utils.FileUtils;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class PaimonHiveCatalogTestSuite extends HiveCatalogTestSuite {
    @Override
    public void registerPaimonHiveCatalog() {
        tableEnv.executeSql("CREATE CATALOG mypaimon WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'metastore' = 'hive',\n" +
                "    'hive-conf-dir' = 'file://" + FileUtils.getHiveConfDir(true) + "',\n" +
                "    'warehouse' = 'file://" + FileUtils.getLakehouseDefaultPath() + "'\n" +
                ");\n");
    }

    @Test
    public void testPaimonHiveCatalog() throws ExecutionException, InterruptedException, DatabaseNotExistException {
        registerDataGenInHiveCatalog();
        tableEnv.executeSql("use catalog mypaimon;");
//        System.out.println(FileUtils.getLakehouseDefaultPath() + "/paimon_test.db");
//        FileUtils.clearDir(FileUtils.getLakehouseDefaultPath() + "/paimon_test.db", false);
//        tableEnv.executeSql("drop database if  exists paimon_test");
        tableEnv.executeSql("create database if not exists paimon_test");
        tableEnv.executeSql("CREATE TABLE if not exists mypaimon.paimon_test.bucket_table (\n" +
                "  `id` Int PRIMARY KEY NOT ENFORCED,\n" +
                "  `name` String,\n" +
                "  `age` Int,\n" +
                "  `create_time` Timestamp\n" +
                ") with  (\n" +
                " 'bucket' = '-1',\n" +
                " 'sink.parallelism' = '1' \n" +
                ");");
        String sql = "insert into mypaimon.paimon_test.bucket_table(id, name, age, create_time) " +
                "select id, name, age, create_time from myhive.test.datagen1";
        System.out.println(sql);
        tableEnv.executeSql(sql)
                .await();
    }

    @Test
    public void testQueryFromHiveCatalog() {
        tableEnv.sqlQuery("select count(1) from mypaimon.paimon_test.bucket_table")
                .execute()
                .print();
    }
}
