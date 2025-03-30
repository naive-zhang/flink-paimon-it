package com.fishsun;

import com.fishsun.conf.JdbcReadConf;
import com.fishsun.conf.PaimonTableConf;
import com.fishsun.conf.SparkTaskConf;
import com.fishsun.utils.FileUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.fishsun.conf.JdbcReadConf.toJdbcReadConf;
import static com.fishsun.conf.PaimonTableConf.CATALOG_NAME;
import static com.fishsun.conf.PaimonTableConf.DB_NAME;
import static com.fishsun.conf.PaimonTableConf.FULL_DB_NAME;
import static com.fishsun.conf.PaimonTableConf.toPaimonTableConf;
import static com.fishsun.conf.SparkTaskConf.toSparkTaskConf;
import static com.fishsun.utils.FileUtils.parseJsonFromFile;
import static com.fishsun.utils.SchemaUtils.genInitSql;
import static com.fishsun.utils.SchemaUtils.genTableName;
import static com.fishsun.utils.SchemaUtils.injectSchema;
import static com.fishsun.utils.SchemaUtils.toDefaultPaimonTable;
import static com.fishsun.utils.SchemaUtils.toPaimonSchema;

public class PaimonHiveCatalog {

    public static Map<String, String> getTaskParams() {
        Map<String, String> taskParams = new HashMap<>();
        // spark 相关的配置
        taskParams.put("app.name", "Spark Paimon with Hive Catalog");
        taskParams.put("master", "local[*]");
        taskParams.put("spark.sql.catalog.paimon.warehouse", FileUtils.getWarehousePath());
        taskParams.put("spark.sql.catalog.paimon.uri", "thrift://localhost:9083");
        // paimon 表相关的配置
        taskParams.put("sys_name", "local");
        taskParams.put("db_name", "test");
        taskParams.put("table_name", "user_profile");
        taskParams.put("partition_source", "created_at");
        taskParams.put("pk", "id, dt");
        taskParams.put("meta_timestamp", "updated_at");
        // jdbc 相关的配置
        taskParams.put("url", "jdbc:mysql://localhost:3306/inventory");
        taskParams.put("username", "root");
        taskParams.put("password", "123456");
        taskParams.put("table", "user_profile");
        taskParams.put("db_table", "user_profile");
        taskParams.put("partition_column", "updated_at");
        taskParams.put("lower_bound", "2024-01-01");
        taskParams.put("upper_bound", "2025-03-31");
        taskParams.put("num_partitions", "500");

        return taskParams;
    }


    public static void main(String[] args) {
        // 获得参数
        Map<String, String> taskParams =
                parseJsonFromFile(args[0]);

        // 获得 spark相关的配置
        SparkTaskConf sparkTaskConf = toSparkTaskConf(taskParams);
        // 生成 sparkSession
        SparkSession spark = sparkTaskConf.toSparkSession();
        // get paimon table conf
        PaimonTableConf paimonTableConf = toPaimonTableConf(taskParams);
        // get jdbc conf
        JdbcReadConf jdbcReadConf = toJdbcReadConf(taskParams);
        // init database
        initDatabase(spark);
        // show databases
        showDatabases(spark);

        // show tables
        showTables(spark, CATALOG_NAME, DB_NAME);

        // get jdbc table
        Dataset<Row> jdbcDataSet = readFromJdbc(spark, jdbcReadConf);

        jdbcDataSet.explain();

        // 转成相应的paimon的格式
        List<StructField> paimonSchema = toPaimonSchema(jdbcDataSet);

        // 注入schema和注释信息
        paimonSchema =

                injectSchema(paimonSchema, jdbcReadConf, paimonTableConf);

        String ddl = toDefaultPaimonTable(paimonSchema, paimonTableConf);
        System.out.println(ddl);
        spark.sql(ddl);
        // show tables
        showTables(spark, CATALOG_NAME, DB_NAME);
        // desc table
        spark.sql("desc formatted " + FULL_DB_NAME + "." +
                        genTableName(paimonTableConf)).
                show(false);
        // getSchemas
        spark.sql("desc formatted " + FULL_DB_NAME + "." +
                        genTableName(paimonTableConf)).
                registerTempTable("schema_tbl");
        // 写入数据
        jdbcDataSet.registerTempTable("ods_tbl");
        String initSql = genInitSql(paimonSchema, paimonTableConf);
        System.out.println(initSql);
        spark.sql(initSql).show(false);
        spark.sql("select count(1) from paimon.paimon_ods." +
                        genTableName(paimonTableConf)).
                show(false);
        spark.sql("select * from paimon.paimon_ods." +
                        genTableName(paimonTableConf) +
                        " order by updated_at desc limit 20").
                show(false);

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
        spark.sql("show databases").show();
    }

    public static void showTables(SparkSession spark, String catalog, String database) {
        spark.sql("show tables from " + catalog + "." + database).show(false);
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
                        .option("numPartitions", jdbcReadConf.getNumPartitions())
                        .load();
            }
            return reader.load();
        }
    }


}