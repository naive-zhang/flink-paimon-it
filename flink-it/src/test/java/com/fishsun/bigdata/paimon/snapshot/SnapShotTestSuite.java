package com.fishsun.bigdata.paimon.snapshot;

import com.fishsun.bigdata.PaimonBasicTestSuite;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class SnapShotTestSuite extends PaimonBasicTestSuite {
    @Override
    public void registerDataGen() {
        try {
            tableEnv.executeSql("create database if not exists test");
            tableEnv.executeSql("CREATE TABLE if not exists test.snapshot_black(\n" +
                    "  `id` Int PRIMARY KEY NOT ENFORCED,\n" +
                    "  `name` String,\n" +
                    "  `age` Int\n" +
                    ") with (\n" +
                    "'connector' = 'datagen',\n" +
                    "'fields.id.kind' = 'random',\n" +
                    "'fields.id.min' = '1',\n" +
                    "'fields.id.max' = '100',\n" +
                    "'fields.name.length' = '10',\n" +
                    "'fields.age.min' = '18',\n" +
                    "'fields.age.max' = '60',\n" +
                    "'rows-per-second' = '20'\n" +
                    ")").await();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        try {
            tableEnv.executeSql("CREATE TABLE if not exists paimon.test.ods_snapshot_test(\n" +
                            " `id`  Int,\n" +
                            " `name` String,\n" +
                            " `age` Int,\n" +
                            " `dt`  string,\n" +
                            "   PRIMARY KEY (id,dt) NOT ENFORCED\n" +
                            ") partitioned by (dt) with  (\n" +
                            "   'changelog-producer' = 'input',\n" +
                            "   'snapshot.time-retained' = '300 s',\n" +
                            "   'snapshot.num-retained.min' = '3',\n" +
                            "   'snapshot.num-retained.max' = '5',\n" +
                            "   'bucket' ='1'\n" +
                            ")")
                    .await();
            tableEnv.executeSql("CREATE TABLE if not exists paimon.test.dwd_snapshot_test(\n" +
                    "`id` bigint,\n" +
                    "  `name` String,\n" +
                    "  `age` Int,\n" +
                    "  `dt`  string,\n" +
                    "   PRIMARY KEY (id,dt) NOT ENFORCED\n" +
                    ") partitioned by (dt) with  (\n" +
                    "   'changelog-producer' = 'input',\n" +
                    "   'bucket' ='1'\n" +
                    ");").await();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testInsertOdsMockData() throws ExecutionException, InterruptedException {
        tableEnv.executeSql("insert into paimon.test.ods_snapshot_test\n" +
                "select id,name,age,'29250110'dt from default_catalog.test.snapshot_black;")
                .await();

    }

    @Test
    public void testInsertDwdMockData() throws ExecutionException, InterruptedException {
        tableEnv.executeSql("insert into paimon.test.dwd_snapshot_test\n" +
                        "select id,name,age,dt from paimon.test.ods_snapshot_test;")
                .await();
    }
}
