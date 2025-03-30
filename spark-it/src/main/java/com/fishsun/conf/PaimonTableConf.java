package com.fishsun.conf;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Author: zhangxinsen
 * @Date: 2025/3/30 00:26
 * @Desc:
 * @Version: v1.0
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaimonTableConf {
    public static final String CATALOG_NAME = "paimon";
    public static final String DB_NAME = "paimon_ods";
    public static final String FULL_DB_NAME = CATALOG_NAME + "." + DB_NAME;

    public static final String SYS_NAME_KEY = "sys_name";
    public static final String DB_NAME_KEY = "db_name";
    public static final String TABLE_NAME_KEY = "table_name";
    public static final String PARTITION_FROM_COLUMN_KEY = "partition_source";
    public static final String PRIMARY_KEYS_KEY = "pk";
    public static final String META_TIMESTAMP_KEY = "meta_timestamp";

    // 不可变的属性
    private String sysName;
    private String dbName;
    private String tableName;
    private String partitionFromColumn;
    private List<String> primaryKeys;
    private String metaTimestampColumn;

    public static PaimonTableConf toPaimonTableConf(Map<String, String> taskParams) {
        if (taskParams == null) {
            throw new IllegalArgumentException("task params is null when toPaimonTableConf");
        }
        PaimonTableConf.PaimonTableConfBuilder builder = PaimonTableConf.builder();
        if (taskParams.containsKey(SYS_NAME_KEY)) {
            builder.sysName(taskParams.get(SYS_NAME_KEY));
        } else {
            throw new IllegalArgumentException(SYS_NAME_KEY + " is null when toPaimonTableConf");
        }
        if (taskParams.containsKey(DB_NAME_KEY)) {
            builder.dbName(taskParams.get(DB_NAME_KEY));
        } else {
            throw new IllegalArgumentException(DB_NAME + " is null when toPaimonTableConf");
        }
        if (taskParams.containsKey(TABLE_NAME_KEY)) {
            builder.tableName(taskParams.get(TABLE_NAME_KEY));
        } else {
            throw new IllegalArgumentException(TABLE_NAME_KEY + " is null when toPaimonTableConf");
        }
        if (taskParams.containsKey(PARTITION_FROM_COLUMN_KEY)) {
            builder.partitionFromColumn(taskParams.get(PARTITION_FROM_COLUMN_KEY));
        } else {
            throw new IllegalArgumentException(PARTITION_FROM_COLUMN_KEY + " is null when toPaimonTableConf");
        }
        if (taskParams.containsKey(PRIMARY_KEYS_KEY)) {
            builder.primaryKeys(Arrays.stream(taskParams.get(PRIMARY_KEYS_KEY).split(",")).map(String::trim).collect(
                    Collectors.toList()));
        } else {
            throw new IllegalArgumentException(PRIMARY_KEYS_KEY + " is null when toPaimonTableConf");
        }
        if (taskParams.containsKey(META_TIMESTAMP_KEY)) {
            builder.metaTimestampColumn(taskParams.get(META_TIMESTAMP_KEY));
        } else {
            throw new IllegalArgumentException(META_TIMESTAMP_KEY + " is null when toPaimonTableConf");
        }
        return builder.build();
    }
}
