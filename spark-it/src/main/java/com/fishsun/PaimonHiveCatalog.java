package com.fishsun;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PaimonHiveCatalog {



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

        spark.sql("show databases").show();



        // 结束 Spark
        spark.stop();
    }
}