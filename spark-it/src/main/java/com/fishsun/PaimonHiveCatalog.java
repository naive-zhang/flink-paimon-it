package com.fishsun;

import com.fishsun.conf.JdbcReadConf;
import com.fishsun.utils.FileUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.fishsun.utils.SchemaUtils.*;

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
                .config("spark.sql.catalog.paimon.warehouse", FileUtils.getWarehousePath())
                // 如果需要 Spark 的 Hive 支持，可以启用
                .config("spark.sql.catalog.paimon.metastore", "hive")
                .config("spark.sql.catalog.paimon.uri", "thrift://localhost:9083")
                // --conf spark.sql.catalog.paimon.metastore=hive \
                //    --conf spark.sql.catalog.paimon.uri=thrift://<hive-metastore-host-name>:<port>
                .enableHiveSupport()
                .getOrCreate();


        // init database
        initDatabase(spark);
        // show databases
        showDatabases(spark);

        // get jdbc connection
        JdbcReadConf jdbcReadConf = getJdbcReadConf();
        // get jdbc table
        Dataset<Row> jdbcDataSet = readFromJdbc(spark, jdbcReadConf);

        // 转成相应的paimon的格式
        List<StructField> paimonSchema = toPaimonSchema(jdbcDataSet);

        // 注入schema和注释信息
        paimonSchema = injectSchema(paimonSchema, jdbcReadConf);

        String ddl = toDefaultPaimonTable(paimonSchema, jdbcReadConf);
        System.out.println(ddl);
        spark.sql(ddl);
        // show tables
        spark.sql("show tables from paimon.paimon_ods").show(false);
        // desc table
        spark.sql("desc formatted paimon_ods.ods_xxx_user_profile").show(false);
        // 写入数据
        jdbcDataSet.registerTempTable("ods_tbl");
        spark.sql("insert into paimon_ods.ods_xxx_user_profile " +
                "select id,\n" +
                "name,\n" +
                "age,\n" +
                "gender,\n" +
                "birthday,\n" +
                "balance,\n" +
                "address,\n" +
                "details,\n" +
                "created_at,\n" +
                "updated_at,\n" +
                "last_login,\n" +
                "last_login_date,\n" +
                "date(created_at) from ods_tbl");

        // 结束 Spark
        spark.stop();
    }


    /**
     * 初始化paimon ods
     *
     * @param spark
     */
    public static void initDatabase(SparkSession spark) {
        spark.sql("create database if not exists paimon.paimon_ods");
    }

    /**
     * 打印数据库
     */
    public static void showDatabases(SparkSession spark) {
        spark.sql("show databases from paimon").show();
    }


    public static JdbcReadConf getJdbcReadConf() {
        return new JdbcReadConf.Builder()
                .url("jdbc:mysql://localhost:3306/inventory")
                .username("root")
                .password("123456")
                .dbTable("user_profile")
                .isPartitionTable(true)
                .partitionFromColumn("created_at")
                .primaryKeys(Arrays.asList("id", "dt"))
                .build();

    }


    /**
     * 返回JDBC连接的数据集
     *
     * @param spark
     * @param jdbcReadConf
     * @return
     */
    public static Dataset<Row> readFromJdbc(SparkSession spark, JdbcReadConf jdbcReadConf) {
        if (jdbcReadConf == null) {
            throw new IllegalArgumentException("jdbcReadConf is null");
        }
        if (jdbcReadConf.isUseQuery()) {
            return spark.read()
                    .format("jdbc")
                    .option("url", jdbcReadConf.getUrl())
                    .option("query", jdbcReadConf.getQuery())
                    .option("user", jdbcReadConf.getUsername())
                    .option("password", jdbcReadConf.getPassword())
                    .load();
        } else {
            DataFrameReader reader = spark.read()
                    .format("jdbc")
                    .option("url", jdbcReadConf.getUrl())
                    .option("dbtable", jdbcReadConf.getDbTable())
                    .option("user", jdbcReadConf.getUsername())
                    .option("password", jdbcReadConf.getPassword());
            if (jdbcReadConf.isUsePartitionColumn()) {
                return reader.option("partitionColumn", jdbcReadConf.getPartitionColumn())
                        .option("lowerBound", jdbcReadConf.getLowerBound())
                        .option("upperBound", jdbcReadConf.getUpperBound())
                        .load();
            }
            return reader.load();
        }
    }


}