package com.fishsun.bigdata.catalog;

import com.fishsun.bigdata.PaimonBasicTestSuite;
import com.fishsun.bigdata.utils.FileUtils;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.junit.Assert;
import org.junit.Test;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.List;

public class HiveCatalogTestSuite extends PaimonBasicTestSuite {
    @Override
    public void registerHiveCatalog() {
        String name            = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir     = FileUtils.getHiveConfDir(false);

        hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hiveCatalog);
// set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");
    }

    @Test
    public void testHiveCatalog() {
        List<String> databases = hiveCatalog.listDatabases();
        for (String database : databases) {
            System.out.println(database);
        }
        Assert.assertTrue(databases.contains("default"));
    }

    @Test
    public void registerDataGenInHiveCatalog() throws DatabaseNotExistException {
        tableEnv.useCatalog("myhive");
        tableEnv.executeSql("create database if not exists test");
        tableEnv.executeSql("drop table if exists test.datagen1");
        tableEnv.executeSql("CREATE TABLE if not exists myhive.test.datagen1 (\n" +
                "  `id` Int PRIMARY KEY NOT ENFORCED,\n" +
                "  `name` String,\n" +
                "  `age` Int,\n" +
                "  `create_time` Timestamp\n" +
                ") with (\n" +
                "'connector' = 'datagen',\n" +
                "'fields.id.kind' = 'random',\n" +
                "'fields.id.min' = '1',\n" +
                "'fields.id.max' = '20',\n" +
                "'fields.name.length' = '10',\n" +
                "'fields.age.min' = '18',\n" +
                "'fields.age.max' = '60',\n" +
                "'number-of-rows' = '100',\n" +
                "'rows-per-second' = '20'\n" +
                ");");
        Assert.assertTrue( hiveCatalog.listDatabases().contains("test"));
        Assert.assertTrue(hiveCatalog.listTables("test").contains("datagen1"));
    }

    @Test
    public void testQueryFromHiveCatalog() throws DatabaseNotExistException {
        tableEnv.sqlQuery("select * from myhive.test.datagen1")
                .execute()
                .print();
    }
}
