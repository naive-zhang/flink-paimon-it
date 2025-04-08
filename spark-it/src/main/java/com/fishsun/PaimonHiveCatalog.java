package com.fishsun;

import com.fishsun.conf.JdbcReadConf;
import com.fishsun.conf.PaimonTableConf;
import com.fishsun.conf.SparkTaskConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

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
import static com.fishsun.utils.SchemaUtils.makeSchemaLatest;
import static com.fishsun.utils.SchemaUtils.toDefaultPaimonTable;
import static com.fishsun.utils.SchemaUtils.toPaimonSchema;

public class PaimonHiveCatalog {

    public static void main(String[] args) {
        // 获得参数
        Map<String, String> taskParams =
                parseJsonFromFile(args);

        // 获得 spark相关的配置
        SparkTaskConf sparkTaskConf = toSparkTaskConf(taskParams);
        // 生成 sparkSession
        SparkSession spark = sparkTaskConf.toSparkSession();
        // get paimon table conf
        PaimonTableConf paimonTableConf = toPaimonTableConf(taskParams);
        // 获取任务类型相关
        // TaskConf taskConf = toTaskConf(taskParams);
        // get jdbc conf
        JdbcReadConf jdbcReadConf = toJdbcReadConf(taskParams);
        System.out.println("jdbcReadConf = " + jdbcReadConf);
        // init database
        initDatabase(spark);
        // show databases
        showDatabases(spark);

        // show tables
        showTables(spark, CATALOG_NAME, DB_NAME);

        // get jdbc table
        Dataset<Row> jdbcDataSet = readDataSet(spark, jdbcReadConf);

        jdbcDataSet.explain();

        // 转成相应的paimon的格式
        List<StructField> paimonSchema = toPaimonSchema(readFromJdbc(spark, jdbcReadConf));

        // 注入schema和注释信息
        paimonSchema =

                injectSchema(paimonSchema, jdbcReadConf, paimonTableConf);

        // 写入数据
        jdbcDataSet.registerTempTable("ods_tbl");

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
        // 确保schema信息无误
        makeSchemaLatest(spark, paimonSchema, paimonTableConf);

        String initSql = genInitSql(spark, paimonSchema, paimonTableConf, jdbcReadConf);
        System.out.println(initSql);
        spark.sql(initSql).show(false);
        // spark.sql("select count(1) from paimon.paimon_ods." +
        //                genTableName(paimonTableConf)).
        //        show(false);
        spark.sql("select * from paimon.paimon_ods.`" +
                        genTableName(paimonTableConf) +
                        "$snapshots` order by snapshot_id desc limit 20").
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
    public static Dataset<Row> readDataSet(SparkSession spark, JdbcReadConf jdbcReadConf) {
        if (jdbcReadConf == null) {
            throw new IllegalArgumentException("jdbcReadConf is null");
        }

        if (jdbcReadConf.getHiveTableName() != null && !jdbcReadConf.getHiveTableName().isEmpty()) {
            return spark.sql("select * from " + jdbcReadConf.getHiveTableName());
        } else {
            return readFromJdbc(spark, jdbcReadConf);
        }
    }

    public static Dataset<Row> readFromJdbc(SparkSession spark, JdbcReadConf jdbcReadConf) {
        if (jdbcReadConf.isUseQuery()) {
            return spark.read()
                    .format("jdbc")
                    .option("url", jdbcReadConf.getUrl())
                    .option("query", jdbcReadConf.getQuery())
                    .option("user", jdbcReadConf.getUsername())
                    .option("password", jdbcReadConf.getPassword())
                    .option("driver", jdbcReadConf.getDriverClass())
                    .load();
        } else {
            DataFrameReader reader = spark.read()
                    .format("jdbc")
                    .option("url", jdbcReadConf.getUrl())
                    .option("dbtable", jdbcReadConf.getDbTable())
                    .option("user", jdbcReadConf.getUsername())
                    .option("driver", jdbcReadConf.getDriverClass())
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