package com.fishsun.bigdata.paimon;

import com.fishsun.bigdata.PaimonBasicTestSuite;
import org.junit.Before;
import org.junit.Test;


public class BucketTableTest extends PaimonBasicTestSuite {

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
//        tableEnv.executeSql("insert into paimon.test.bucket_tbl_with_2(id, name, age, dt) " +
//                " select id, name, age, date_format(CURRENT_TIMESTAMP,'yyyyMMdd') as dt " +
//                "from  default_catalog.test.datagen1 ");
//        tableEnv.sqlQuery("select id, name, age, dt from paimon.test.bucket_tbl_with_2")
//                .execute().print();
        tableEnv.sqlQuery(
                        "select id, name, age, date_format(CURRENT_TIMESTAMP,'yyyyMMdd') as dt " +
                                "from  default_catalog.test.datagen1 "
                )
                .execute()
                .print();
    }

    @Test
    public void testBucketTableWithDynamicBuckets() {

    }
}
