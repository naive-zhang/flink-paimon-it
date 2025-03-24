package com.fishsun.utils;

import com.fishsun.conf.JdbcReadConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class SchemaUtils {
    public static final String DEFAULT_PARTITION_COLUMN_NAME = "dt";

    public static DataType toPaimonDataType(DataType dataType) {
        if (dataType == DataTypes.TimestampType) {
            return DataTypes.TimestampNTZType;
        }
        return dataType;
    }

    /**
     * 直接将类型转化成paimon相应的格式数据
     *
     * @param dataSet
     * @return
     */
    public static List<StructField> toPaimonSchema(Dataset<Row> dataSet) {
        List<StructField> fields = new ArrayList<>();
        dataSet.schema().toList().foreach(
                fields::add
        );
        return fields.stream().map(field -> new StructField(
                field.name(),
                toPaimonDataType(field.dataType()),
                true,
                null
        )).collect(Collectors.toList());
    }

    /**
     * 获得表的注释信息
     *
     * @param jdbcReadConf
     * @return
     */
    private static Map<String, String> getTableComment(JdbcReadConf jdbcReadConf) {
        Map<String, String> comments = new HashMap<>();
        String query;

        if (jdbcReadConf.getUrl().startsWith("jdbc:mysql")) {
            query = "SELECT COLUMN_NAME, COLUMN_COMMENT AS COMMENTS FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + jdbcReadConf.getDbTable() + "'";
        } else if (jdbcReadConf.getUrl().startsWith("jdbc:oracle")) {
            query = "SELECT COLUMN_NAME, COMMENTS FROM USER_COL_COMMENTS WHERE TABLE_NAME = '" + jdbcReadConf.getDbTable().toUpperCase() + "'";
        } else if (jdbcReadConf.getUrl().startsWith("jdbc:postgresql")) {
            query = "SELECT a.attname AS COLUMN_NAME, d.description AS COMMENTS " +
                    "FROM pg_attribute a " +
                    "JOIN pg_class c ON a.attrelid = c.oid " +
                    "LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum " +
                    "WHERE c.relname = '" + jdbcReadConf.getDbTable() + "' AND a.attnum > 0";
        } else {
            throw new UnsupportedOperationException("Unsupported database: " + jdbcReadConf.getUrl());
        }
        Properties props = new Properties();
        props.setProperty("user", jdbcReadConf.getUsername());
        props.setProperty("password", jdbcReadConf.getPassword());

        try (Connection conn = DriverManager.getConnection(jdbcReadConf.getUrl(), props);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                String comment = rs.getString("COMMENTS") != null ? rs.getString("COMMENTS") : "";
                comments.put(colName, comment);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return comments;
    }

    /**
     * 找到对应的分区键
     *
     * @param paimonSchema
     * @return
     */
    private static StructField findPartitionColumn(List<StructField> paimonSchema, JdbcReadConf jdbcReadConf) {
        Optional<StructField> partitionField = paimonSchema.stream().filter(
                x -> x.name().equals(jdbcReadConf.getPartitionFromColumn())
        ).findFirst();
        if (!partitionField.isPresent()) {
            throw new IllegalArgumentException("partition field not found, " + jdbcReadConf.getPartitionFromColumn());
        }
        return partitionField.get();
    }

    /**
     * 注入分区信息
     * 如果分区不存在则抛出异常
     * 如果分区存在并且类型不是时间类型, 则直接写入即可
     * 否则用dt分区
     *
     * @param paimonSchema
     * @param jdbcReadConf
     * @return
     */
    public static List<StructField> injectSchema(List<StructField> paimonSchema, JdbcReadConf jdbcReadConf) {
        Map<String, String> tableComment = getTableComment(jdbcReadConf);
        List<StructField> newPaimonSchema = paimonSchema.stream().map(
                structField -> new StructField(
                        structField.name(),
                        structField.dataType(),
                        true,
                        new MetadataBuilder()
                                .putString("comment", tableComment.get(structField.name()))
                                .build()
                )
        ).collect(Collectors.toList());
        if (!jdbcReadConf.getIsPartitionTable()) {
            return newPaimonSchema;
        }
        StructField partitionField = findPartitionColumn(newPaimonSchema, jdbcReadConf);
        if (partitionField.dataType() == DataTypes.TimestampNTZType) {
            newPaimonSchema.add(new StructField(
                    DEFAULT_PARTITION_COLUMN_NAME,
                    DataTypes.DateType,
                    true,
                    new MetadataBuilder()
                            .putString("comment", "分区")
                            .build()
            ));
        }
        return newPaimonSchema;
    }

    public static String toDefaultPaimonTable(List<StructField> paimonSchema, JdbcReadConf jdbcReadConf) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS paimon.paimon_ods.ods_xxx_");
        sb.append(jdbcReadConf.getDbTable());
        sb.append("(\n");
        List<String> filedList = new ArrayList<>();
        for (StructField x : paimonSchema) {
            StringBuilder sb2 = new StringBuilder();
            sb2.append(x.name());
            sb2.append(" ");
            sb2.append(x.dataType().sql());
            sb2.append(" ");
            sb2.append("COMMENT '");
            sb2.append(x.getComment().get());
            sb2.append("'");
            filedList.add(sb2.toString());
        }
        sb.append(String.join(",\n", filedList));
        sb.append(")\n");
//        sb.append("USING paimon \n");
        StructField partitionField = findPartitionColumn(paimonSchema, jdbcReadConf);
        String partitionColumnName = partitionField.dataType() != DataTypes.TimestampNTZType ? partitionField.name() : DEFAULT_PARTITION_COLUMN_NAME;
        sb.append("PARTITIONED BY (");
        sb.append(partitionColumnName);
        sb.append(") location '");
        sb.append(FileUtils.getWarehousePath() + "/paimon_ods.db/ods_xxx_" + jdbcReadConf.getDbTable());
        sb.append("' TBLPROPERTIES (\n");
        if (!jdbcReadConf.getPrimaryKeys().isEmpty()) {
            sb.append("'primary-key' = '");
            sb.append(String.join(",", jdbcReadConf.getPrimaryKeys()));
            sb.append("',\n");
        }
        sb.append("'bucket' = '8',\n" +
                "'changelog-producer' = 'lookup',\n" +
                "'snapshot.num-retained.max' = '18',\n" +
                "'snapshot.num-retained.min' = '6',\n" +
                "'snapshot.time-retained' = '2min',\n" +
                "'tag.automatic-creation' = 'process-time',\n" +
                "'tag.creation-delay' = '600000',\n" +
                "'tag.creation-period' = 'hourly',\n" +
                "'tag.num-retained-max' = '90'\n" +
                ")");
        /**
         * "PARTITIONED BY (dt) TBLPROPERTIES (\n" +
         *                 "    'primary-key' = 'id, dt',\n" +
         *                 "    'bucket' = '-1',\n" +
         *                 "    'changelog-producer' = 'lookup',\n" +
         *                 "    'snapshot.num-retained.max' = '18',\n" +
         *                 "  'snapshot.num-retained.min' = '6',\n" +
         *                 "  'snapshot.time-retained' = '2min',\n" +
         *                 "  'tag.automatic-creation' = 'process-time',\n" +
         *                 "  'tag.creation-delay' = '600000',\n" +
         *                 "  'tag.creation-period' = 'hourly',\n" +
         *                 "  'tag.num-retained-max' = '90'\n" +
         *                 ");")
         */
        return sb.toString();
    }
}
