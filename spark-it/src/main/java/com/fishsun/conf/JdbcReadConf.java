package com.fishsun.conf;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

import static com.fishsun.utils.TimeUtils.calculateSegments;

/**
 * 必须指定 tableName
 */

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class JdbcReadConf {

    public static final String URL_KEY = "url";
    public static final String USERNAME_KEY = "username";
    public static final String PASSWORD_KEY = "password";
    public static final String TABLE_NAME_KEY = "table";
    public static final String QUERY_KEY = "query";
    public static final String DB_TABLE_KEY = "db_table";
    public static final String PARTITION_COLUMN_KEY = "partition_column";
    public static final String LOWER_BOUND_KEY = "lower_bound";
    public static final String UPPER_BOUND_KEY = "upper_bound";
    public static final String NUM_PARTITIONS_KEY = "num_partitions";
    public static final String JDBC_DRIVER_CLASS_KEY = "jdbc_driver_class";
    public static final String TIME_INTERVAL_KEY = "time_interval";
    public static final String HIVE_TABLE_NAME_KEY = "hive_table";

    // 不可变的属性
    private String url;
    private String driverClass; // infer
    private String username;
    private String password;

    private String tableName;

    private String hiveTableName;

    private String query;
    private String dbTable;
    private String partitionColumn;
    private String lowerBound;
    private String upperBound;
    private int numPartitions;
    private boolean isUseQuery; // infer
    private boolean isUsePartitionColumn; // infer

    public static JdbcReadConf toJdbcReadConf(Map<String, String> taskParams) {
        if (taskParams == null) {
            throw new IllegalArgumentException("task params is null when toJdbcReadConf");
        }
        JdbcReadConfBuilder builder = JdbcReadConf.builder();
        if (taskParams.containsKey(URL_KEY)) {
            builder.url(taskParams.get(URL_KEY));
        } else {
            throw new IllegalArgumentException(URL_KEY + " is null when toJdbcReadConf");
        }
        if (taskParams.containsKey(USERNAME_KEY)) {
            builder.username(taskParams.get(USERNAME_KEY));
        } else {
            throw new IllegalArgumentException(USERNAME_KEY + " is null when toJdbcReadConf");
        }
        if (taskParams.containsKey(PASSWORD_KEY)) {
            builder.password(taskParams.get(PASSWORD_KEY));
        } else {
            throw new IllegalArgumentException(PASSWORD_KEY + " is null when toJdbcReadConf");
        }
        if (taskParams.containsKey(TABLE_NAME_KEY)) {
            builder.tableName(taskParams.get(TABLE_NAME_KEY));
        } else {
            throw new IllegalArgumentException(TABLE_NAME_KEY + "tableName is null when toJdbcReadConf");
        }

        if (taskParams.containsKey(QUERY_KEY)) {
            builder.query(taskParams.get(QUERY_KEY));
        }

        if (taskParams.containsKey(DB_TABLE_KEY)) {
            builder.dbTable(taskParams.get(DB_TABLE_KEY));
        }

        if (taskParams.containsKey(HIVE_TABLE_NAME_KEY)) {
            builder.hiveTableName(taskParams.get(HIVE_TABLE_NAME_KEY));
        }

        if (taskParams.containsKey(QUERY_KEY) && taskParams.containsKey(DB_TABLE_KEY) &&
                taskParams.containsKey(HIVE_TABLE_NAME_KEY)) {
            throw new IllegalArgumentException(
                    "only one of " + QUERY_KEY + ", " + HIVE_TABLE_NAME_KEY + " and " + DB_TABLE_KEY + " can be set");
        }

        if (!taskParams.containsKey(QUERY_KEY) && !taskParams.containsKey(DB_TABLE_KEY) &&
                !taskParams.containsKey(HIVE_TABLE_NAME_KEY)) {
            throw new IllegalArgumentException(
                    "One of " + QUERY_KEY + ", " + HIVE_TABLE_NAME_KEY + " and " + DB_TABLE_KEY + " should be set");
        }

        if (taskParams.containsKey(PARTITION_COLUMN_KEY)) {
            builder.partitionColumn(taskParams.get(PARTITION_COLUMN_KEY));
            if (taskParams.containsKey(UPPER_BOUND_KEY)) {
                builder.upperBound(taskParams.get(UPPER_BOUND_KEY));
            } else {
                throw new IllegalArgumentException(
                        UPPER_BOUND_KEY + " is null when toJdbcReadConf and " + DB_TABLE_KEY + " is set");
            }
            if (taskParams.containsKey(LOWER_BOUND_KEY)) {
                builder.lowerBound(taskParams.get(LOWER_BOUND_KEY));
            } else {
                throw new IllegalArgumentException(
                        LOWER_BOUND_KEY + " is null when toJdbcReadConf and " + DB_TABLE_KEY + " is set");
            }
            if (taskParams.containsKey(TIME_INTERVAL_KEY)) {
                builder.numPartitions(
                        calculateSegments(taskParams.get(LOWER_BOUND_KEY), taskParams.get(UPPER_BOUND_KEY),
                                Integer.parseInt(taskParams.get(TIME_INTERVAL_KEY))));
            } else if (taskParams.containsKey(NUM_PARTITIONS_KEY)) {
                builder.numPartitions(Integer.parseInt(taskParams.get(NUM_PARTITIONS_KEY)));
            } else {
                builder.numPartitions(200);
            }
        }
        if (taskParams.containsKey(JDBC_DRIVER_CLASS_KEY)) {
            builder.driverClass(taskParams.get(JDBC_DRIVER_CLASS_KEY));
        } else {
            builder.driverClass(inferDriverClass(taskParams.get(URL_KEY)));
        }
        builder.isUseQuery(true);
        builder.isUsePartitionColumn(true);
        JdbcReadConf jdbcReadConf = builder.build();
        jdbcReadConf.checkIsUseQuery();
        return jdbcReadConf;
    }

    private void checkIsUseQuery() {
        if (query == null || query.isEmpty()) {
            isUseQuery = false;
        } else {
            isUseQuery = true;
        }
        if (isUseQuery) {
            isUsePartitionColumn = false;
        } else {
            if (partitionColumn == null || partitionColumn.isEmpty()) {
                isUsePartitionColumn = false;
                ;
            } else {
                isUsePartitionColumn = true;
            }
        }
    }

    // 根据url推断driverClass的私有方法
    private static String inferDriverClass(String url) {
        // 检查url是否有效
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("URL cannot be null or empty");
        }
        // 根据URL前缀推断驱动类
        if (url.startsWith("jdbc:mysql:")) {
            return "com.mysql.cj.jdbc.Driver";        // MySQL驱动
        } else if (url.startsWith("jdbc:postgresql:")) {
            return "org.postgresql.Driver";        // PostgreSQL驱动
        } else if (url.startsWith("jdbc:oracle:")) {
            return "oracle.jdbc.driver.OracleDriver";  // Oracle驱动
        } else {
            throw new IllegalArgumentException("Cannot infer driver class from url: " + url);
        }
    }
}

