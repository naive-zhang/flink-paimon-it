package com.fishsun.conf;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @Author: zhangxinsen
 * @Date: 2025/3/30 10:40
 * @Desc:
 * @Version: v1.0
 */

@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SparkTaskConf {
    public static final String APP_NAME_KEY = "app.name";
    public static final String MASTER_KEY = "master";
    public static final String WAREHOUSE_KEY = "spark.sql.catalog.paimon.warehouse";
    public static final String PAIMON_URI_KEY = "spark.sql.catalog.paimon.uri";
    public static final String DEFAULT_APP_NAME = "Spark Paimon with Hive Catalog";
    public static final String DEFAULT_MASTER = "local[*]";

    public static final List<String> CONFIG_KEY_LIST = Arrays.asList(
            APP_NAME_KEY,
            MASTER_KEY,
            WAREHOUSE_KEY,
            PAIMON_URI_KEY
    );

    public String master;
    public String warehouse;
    public String uri;
    public String appName;

    public static SparkTaskConf toSparkTaskConf(Map<String, String> taskParams) {
        if (taskParams == null) {
            throw new IllegalArgumentException("task params is null when toSparkTaskConf");
        }
        SparkTaskConfBuilder builder = SparkTaskConf.builder();
        builder.master(taskParams.getOrDefault(MASTER_KEY, DEFAULT_MASTER));
        if (taskParams.containsKey(WAREHOUSE_KEY)) {
            builder.warehouse(taskParams.get(WAREHOUSE_KEY));
        } else {
            throw new IllegalArgumentException(WAREHOUSE_KEY + " is null when toSparkTaskConf");
        }
        if (taskParams.containsKey(PAIMON_URI_KEY)) {
            builder.uri(taskParams.get(PAIMON_URI_KEY));
        } else {
            throw new IllegalArgumentException(PAIMON_URI_KEY + " is null when toSparkTaskConf");
        }
        builder.appName(taskParams.getOrDefault(APP_NAME_KEY, DEFAULT_APP_NAME));
        return builder.build();
    }

    /**
     * warehouse和 uri必须指定
     * appName和 master取默认值
     *
     * @return
     */
    public SparkSession toSparkSession() {
        if (warehouse == null) {
            throw new IllegalArgumentException("warehouse is null when toSparkSession");
        }
        if (uri == null) {
            throw new IllegalArgumentException("uri is null when toSparkSession");
        }
        return SparkSession.builder()
                .appName(appName)
                // 如果是本地测试，可以加上 master
                .master(master)
                // 配置 Paimon Catalog
                .config("spark.sql.extensions", "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
                // 这里的 "paimon" 是我们在 spark.sql.catalog 中自定义的 catalog 名称
                .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
                // 指定 Paimon 仓库位置（可以是 HDFS / S3 / 本地文件系统等）
                .config("spark.sql.catalog.paimon.warehouse", warehouse)
                // 如果需要 Spark 的 Hive 支持，可以启用
                .config("spark.sql.catalog.paimon.metastore", "hive")
                .config("spark.sql.catalog.paimon.uri", uri)
                // --conf spark.sql.catalog.paimon.metastore=hive \
                //    --conf spark.sql.catalog.paimon.uri=thrift://<hive-metastore-host-name>:<port>
                .enableHiveSupport()
                .getOrCreate();
    }
}
