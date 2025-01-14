package com.fishsun;

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
                 .enableHiveSupport()
                 .getOrCreate();

         spark.sql("use paimon");

         // 示例: 查看 Paimon Catalog 中的 database
         spark.sql("SHOW DATABASES").show();

         // 也可以查看所有表
          spark.sql("SHOW TABLES in paimon_test").show();

          // 查询表结构
        System.out.println(spark.sql("show create table paimon_test.bucket_table").first().getString(0));

          // 查询表里面的数据
        spark.sql("select * from paimon_test.bucket_table").show();

        // 查询有多少数据
        spark.sql("select count(1) from paimon_test.bucket_table").show();
        // 查询快照数据
        spark.sql("select * from paimon_test.bucket_table VERSION AS OF 1;").show();
        spark.sql("select * from paimon_test.bucket_table VERSION AS OF 6;").show();
        // 尝试增量读取
        spark.sql("select * from paimon_incremental_query('paimon_test.bucket_table', 1, 6)").show();
//        spark.sql("select * from paimon_incremental_query(paimon_test.bucket_table,1,2)").show();
         // 更多操作例如创建表、写入数据、查询等:
         // spark.sql("CREATE TABLE paimon.default.my_table (id INT, name STRING) USING paimon");
         // spark.sql("INSERT INTO paimon.default.my_table VALUES (1, 'Alice'), (2, 'Bob')");
         // spark.sql("SELECT * FROM paimon.default.my_table").show();

         // 结束 Spark
         spark.stop();
    }
}