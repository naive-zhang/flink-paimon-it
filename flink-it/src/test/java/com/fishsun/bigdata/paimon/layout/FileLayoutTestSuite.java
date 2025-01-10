package com.fishsun.bigdata.paimon.layout;

import com.fishsun.bigdata.PaimonBasicTestSuite;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class FileLayoutTestSuite extends PaimonBasicTestSuite {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        try {
            tableEnv.executeSql("create database if not exists layout").await();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        try {
            tableEnv.executeSql("CREATE TABLE if not exists paimon.layout.layout1(\n" +
                            " `id` Int,\n" +
                            "  `name` String,\n" +
                            "  `age` Int,\n" +
                            "   `dt` String,\n" +
                            "   PRIMARY KEY (id,dt) NOT ENFORCED\n" +
                            ") PARTITIONED BY (dt) with  (\n" +
                            "'bucket' = '1',\n" +
                            "'changelog-producer'='input'\n" +
                            ");")
                    .await();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void registerDataGen() {
        tableEnv.executeSql("create database test");
        try {
            tableEnv.executeSql("CREATE TABLE if not exists test.input_source (\n" +
                            "  `id` Int PRIMARY KEY NOT ENFORCED,\n" +
                            "  `name` String,\n" +
                            "  `age` Int\n" +
                            ") with (\n" +
                            "'connector' = 'datagen',\n" +
                            "'fields.id.kind' = 'random',\n" +
                            "'fields.id.min' = '1',\n" +
                            "'fields.id.max' = '5',\n" +
                            "'fields.name.length' = '10',\n" +
                            "'fields.age.min' = '18',\n" +
                            "'fields.age.max' = '60',\n" +
                            "'rows-per-second' = '10'\n" +
                            ")")
                    .await();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testQueryFromInputSource() {
        tableEnv.sqlQuery("select * from " +
                        "default_catalog.test.input_source")
                .execute()
                .print();
    }

    @Test
    public void testInsertSomeDataIntoLayoutTbl() throws ExecutionException, InterruptedException {
        tableEnv.executeSql("insert into paimon.layout.layout1  \n" +
                "select id,name,age,'20250110' from default_catalog.test.input_source")
                .await();
    }
}
