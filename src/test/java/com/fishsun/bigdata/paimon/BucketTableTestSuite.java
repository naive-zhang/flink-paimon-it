package com.fishsun.bigdata.paimon;

import com.fishsun.bigdata.PaimonBasicTestSuite;
import org.junit.Before;
import org.junit.Test;


public class BucketTableTestSuite extends PaimonBasicTestSuite {

    @Before
    public void setUp() {
        super.setUp();
    }


    @Test
    public void testGetFromDatagen1() {
        tableEnv.sqlQuery("select id, count(1) as cnt, avg(age) as avg_age" +
                " from default_catalog.test.datagen1 " +
                "group by id "
        ).execute().print();
    }

    @Test
    public void testBucketTableWithCertainBucket() {
        tableEnv.executeSql("CREATE TABLE if not exists paimon.test.bucket_tbl_with_2 (\n" +
                "  id bigint,\n" +
                "  name String,\n" +
                "  age Int,\n" +
                "  dt string,\n" +
                "  PRIMARY KEY (id,dt) NOT ENFORCED\n" +
                ") PARTITIONED BY (dt) with  (\n" +
                " 'bucket' = '2',\n" +
                " 'file.format'='avro',\n" +
                " 'sink.parallelism' = '2' \n" +
                ")");
        tableEnv.executeSql("insert into paimon.test.bucket_tbl_with_2(id, name, age, dt) " +
                " select id, name, age, date_format(CURRENT_TIMESTAMP,'yyyyMMdd') as dt " +
                "from  default_catalog.test.datagen1 ");
        tableEnv.sqlQuery("select id, name, age, dt from paimon.test.bucket_tbl_with_2")
                .execute().print();
    }

    @Test
    public void testBucketTableWithDynamicBucketsPartition() {
        tableEnv.executeSql("CREATE TABLE if not exists paimon.test.bucket_tbl_with_dynamic_buckets_partition (\n" +
                "  id bigint,\n" +
                "  name String,\n" +
                "  age Int,\n" +
                "  dt string,\n" +
                "  PRIMARY KEY (id,dt) NOT ENFORCED\n" +
                ") PARTITIONED BY (dt) with  (\n" +
                " 'bucket' = '-1',\n" +
                " 'file.format'='avro',\n" +
                " 'dynamic-bucket.initial-buckets'='4',\n" +
                " 'sink.parallelism' = '2' \n" +
                ");");
        tableEnv.executeSql("insert into paimon.test.bucket_tbl_with_dynamic_buckets_partition values\n" +
                "(1,'zhangsan',18,'2023-01-01'),\n" +
                "(2,'lisi',18,'2023-01-01'),\n" +
                "(1,'zhangsan',18,'2023-01-02'),\n" +
                "(1,'zhangsan',18,'2023-01-03');");
        tableEnv.sqlQuery("select id, name, age, dt from paimon.test.bucket_tbl_with_dynamic_buckets_partition")
                .execute()
                .print();
    }

    @Test
    public void testBucketTableWithDynamicBucketsGlobalUnique() {
        logger.info("create dynamic bucket table and not bind partition into primary keys");
        tableEnv.executeSql("CREATE TABLE if not exists paimon.test.bucket_tbl_with_dynamic_buckets (\n" +
                "  id bigint,\n" +
                "  name String,\n" +
                "  age Int,\n" +
                "  dt string,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") PARTITIONED BY (dt) with  (\n" +
                " 'bucket' = '-1',\n" +
                " 'file.format'='avro',\n" +
                " 'merge-engine' = 'deduplicate',\n" +
                " 'sink.parallelism' = '2' \n" +
                ");");
        logger.info("trying to insert data into table");
        tableEnv.executeSql("insert into paimon.test.bucket_tbl_with_dynamic_buckets values\n" +
                "(1,'zhangsan',18,'2023-01-01')");
        tableEnv.executeSql("insert into paimon.test.bucket_tbl_with_dynamic_buckets values\n" +
                "(1,'zhangsan',18,'2023-01-02')");
        tableEnv.executeSql("insert into paimon.test.bucket_tbl_with_dynamic_buckets values\n" +
                "(1,'zhangsan',18,'2023-01-03')");
        tableEnv.sqlQuery("select id, name, age, dt from paimon.test.bucket_tbl_with_dynamic_buckets")
                .execute()
                .print();
    }
}
