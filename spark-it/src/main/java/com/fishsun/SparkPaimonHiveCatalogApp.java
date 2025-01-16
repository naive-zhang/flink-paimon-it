package com.fishsun;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkPaimonHiveCatalogApp {
    public static void main(String[] args) {
        // // 创建 SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Spark Paimon with Hive Catalog")
                // 如果是本地测试，可以加上 master
                .master("local[*]")
                // 配置 Paimon Catalog
                .config("spark.sql.extensions", "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
                // 这里的 "paimon" 是我们在 spark.sql.catalog 中自定义的 catalog 名称
                .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
                // 指定 Paimon 仓库位置（可以是 HDFS / S3 / 本地文件系统等）
                .config("spark.sql.catalog.paimon.warehouse", "file:///home/fishsun/IdeaProjects/flink-paimon-it/flink-it/lakehouse")
                // 如果需要 Spark 的 Hive 支持，可以启用
                .config("spark.sql.catalog.paimon.metastore", "hive")
                .config("spark.sql.catalog.paimon.uri", "thrift://localhost:9083")
                // --conf spark.sql.catalog.paimon.metastore=hive \
                //    --conf spark.sql.catalog.paimon.uri=thrift://<hive-metastore-host-name>:<port>
                .enableHiveSupport()
                .getOrCreate();


        Dataset<Row> oldTable = spark.read()
                .format("paimon")
                .load("file:///home/fishsun/IdeaProjects/flink-paimon-it/flink-it/lakehouse/paimon_test.db/bucket_table");
        oldTable.show(20, false);

        spark.sql("use paimon");

        // 示例: 查看 Paimon Catalog 中的 database
        spark.sql("SHOW DATABASES").show();

        // 创建表
        spark.sql("create database if not exists paimon_test").show();

        spark.sql("use paimon.paimon_test").show();
        oldTable.registerTempTable("bucket_table");

        // 也可以查看所有表
        spark.sql("SHOW TABLES in paimon_test").show();


        // 查询表结构
        // System.out.println(spark.sql("show create table bucket_table").first().getString(0));

        // 查询表里面的数据
        // spark.sql("select * from paimon_test.bucket_table").show();

        // 查询有多少数据
        spark.sql("select count(1) from bucket_table").show();


        // 创建表
        spark.sql("create table if not exists paimon_test.bucket_table2 (" +
                "id string," +
                "name string," +
                "age int," +
                "create_time Timestamp_ntz," +
                "dt date) " +
                "PARTITIONED BY (dt) TBLPROPERTIES (\n" +
                "    'primary-key' = 'id, dt',\n" +
                "    'bucket' = '-1',\n" +
                "    'changelog-producer' = 'lookup',\n" +
                "    'snapshot.num-retained.max' = '18',\n" +
                "  'snapshot.num-retained.min' = '6',\n" +
                "  'snapshot.time-retained' = '2min',\n" +
                "  'tag.automatic-creation' = 'process-time',\n" +
                "  'tag.creation-delay' = '600000',\n" +
                "  'tag.creation-period' = 'hourly',\n" +
                "  'tag.num-retained-max' = '90'\n" +
                ");").show();
//         spark.sql("insert overwrite paimon_test.bucket_table2(id, name, age, create_time, dt) " +
//                "select id, name, age, create_time, date(create_time) as dt from bucket_table").show();
        String ddl = spark.sql("show create table paimon_test.bucket_table2")
                .first().getString(0);
        System.out.println(ddl);

//        spark.sql("drop table if exists paimon_test.bucket_table2")
//                .collect();

        ddl = spark.sql("show create table paimon_test.bucket_table_flink")
                .first().getString(0);
        System.out.println(ddl);

        // 查询快照数据
        // spark.sql("select * from paimon_test.bucket_table VERSION AS OF 1;").show();
        // spark.sql("select * from paimon_test.bucket_table VERSION AS OF 2;").show();
        // 尝试增量读取
        // spark.sql("select * from paimon_incremental_query('bucket_table', '2025-01-14 21', '2025-01-14 22')").show();
//        spark.sql("select * from paimon_incremental_query(paimon_test.bucket_table,1,2)").show();

        // snapshot表
        // spark.sql("select * from paimon_test.`bucket_table$snapshots`;").show();

        // schema表
        // spark.sql("select * from paimon_test.`bucket_table$schemas`;").show();

        // tag表
        // spark.sql("select * from paimon_test.`bucket_table$tags` order by commit_time;").show();

        // 审计表
        // spark.sql("select * from paimon_test.`bucket_table$audit_log` where rowkind <> '+I';").show();
        // spark.sql("select id, count(1) as cnt " +
        //         "from paimon_test.`bucket_table$audit_log` " +
        //         "group by id " +
        //         "order by cnt desc;").show();

        spark.sql("select * from paimon_test.bucket_table2").show();

        spark.sql("select count(1) from paimon_test.bucket_table2").show();
        spark.sql("select id, count(1) as cnt " +
                         "from paimon_test.`bucket_table2$audit_log` " +
                         "group by id " +
                         "order by cnt desc;").show();
        // 结束 Spark
        spark.stop();
    }
}