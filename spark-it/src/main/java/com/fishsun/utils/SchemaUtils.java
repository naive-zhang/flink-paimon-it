package com.fishsun.utils;

import com.fishsun.conf.JdbcReadConf;
import com.fishsun.conf.PaimonTableConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

public class SchemaUtils {
    public static final String DEFAULT_PARTITION_COLUMN_NAME = "dt";
    public static final String MQ_PARTITION_INDEX_KEY = "partition_index";
    public static final String MQ_PARTITION_OFFSET_KEY = "offset";
    public static final String MQ_META_TIMESTAMP_KEY = "meta_timestamp";
    public static final String IS_CDC_DELETE_KEY = "is_cdc_delete";

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

        if (jdbcReadConf.getUrl().contains("jdbc:mysql")) {
            query =
                    "SELECT COLUMN_NAME, COLUMN_COMMENT AS COMMENTS FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" +
                            jdbcReadConf.getTableName() + "'";
        } else if (jdbcReadConf.getUrl().contains("jdbc:oracle")) {
            query = "SELECT COLUMN_NAME, COMMENTS FROM USER_COL_COMMENTS WHERE TABLE_NAME = '" +
                    jdbcReadConf.getTableName().toUpperCase() + "'";
        } else if (jdbcReadConf.getUrl().contains("jdbc:postgresql")) {
            query = "SELECT a.attname AS COLUMN_NAME, d.description AS COMMENTS " +
                    "FROM pg_attribute a " +
                    "JOIN pg_class c ON a.attrelid = c.oid " +
                    "LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum " +
                    "WHERE c.relname = '" + jdbcReadConf.getTableName() + "' AND a.attnum > 0";
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
    private static StructField findPartitionColumn(List<StructField> paimonSchema, PaimonTableConf paimonTableConf) {
        Optional<StructField> partitionField = paimonSchema.stream().filter(
                x -> x.name().equals(paimonTableConf.getPartitionFromColumn())
        ).findFirst();
        if (!partitionField.isPresent()) {
            throw new IllegalArgumentException(
                    "partition field not found, " + paimonTableConf.getPartitionFromColumn());
        }
        return partitionField.get();
    }

    /**
     * 注入分区信息
     * 如果分区不存在则抛出异常
     * 如果分区存在并且类型不是时间类型, 则直接写入即可
     * 否则用dt分区
     * 同时注入 partitionIdx和 offset这两个辅助字段
     *
     * @param paimonSchema
     * @param jdbcReadConf
     * @param paimonTableConf
     * @return
     */
    public static List<StructField> injectSchema(List<StructField> paimonSchema, JdbcReadConf jdbcReadConf,
                                                 PaimonTableConf paimonTableConf) {
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
        StructField partitionField = findPartitionColumn(newPaimonSchema, paimonTableConf);
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
        newPaimonSchema.add(new StructField(
                MQ_PARTITION_INDEX_KEY,
                DataTypes.LongType,
                true,
                new MetadataBuilder()
                        .putString("comment", "mq 中分区号")
                        .build()
        ));
        newPaimonSchema.add(new StructField(
                MQ_PARTITION_OFFSET_KEY,
                DataTypes.LongType,
                true,
                new MetadataBuilder()
                        .putString("comment", "mq 中offset")
                        .build()
        ));
        newPaimonSchema.add(new StructField(
                MQ_META_TIMESTAMP_KEY,
                DataTypes.TimestampNTZType,
                true,
                new MetadataBuilder()
                        .putString("comment", "mq 写入时间")
                        .build()
        ));
        newPaimonSchema.add(new StructField(
                IS_CDC_DELETE_KEY,
                DataTypes.BooleanType,
                true,
                new MetadataBuilder()
                        .putString("comment", "cdc中是否捕获到物理删除")
                        .build()
        ));
        return newPaimonSchema;
    }

    public static String genTableName(PaimonTableConf paimonTableConf) {
        StringBuilder sb = new StringBuilder();
        sb.append("ods_");
        sb.append(paimonTableConf.getSysName());
        sb.append("_");
        sb.append(paimonTableConf.getDbName());
        sb.append("_");
        sb.append(paimonTableConf.getTableName());
        sb.append("_rt");
        return sb.toString();
    }

    /**
     * 生成对应表的 DDL
     *
     * @param paimonSchema
     * @param paimonTableConf
     * @return
     */
    public static String toDefaultPaimonTable(List<StructField> paimonSchema,
                                              PaimonTableConf paimonTableConf) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS paimon.paimon_ods.");
        sb.append(genTableName(paimonTableConf));
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
        StructField partitionField = findPartitionColumn(paimonSchema, paimonTableConf);
        String partitionColumnName = partitionField.dataType() != DataTypes.TimestampNTZType ? partitionField.name() : DEFAULT_PARTITION_COLUMN_NAME;
        sb.append("PARTITIONED BY (");
        sb.append(partitionColumnName);
        sb.append(") location '");
        sb.append(FileUtils.getWarehousePath() + "/paimon_ods.db/" + genTableName(paimonTableConf));
        sb.append("' TBLPROPERTIES (\n");
        if (!paimonTableConf.getPrimaryKeys().isEmpty()) {
            sb.append("'primary-key' = '");
            sb.append(String.join(",", paimonTableConf.getPrimaryKeys()));
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
        return sb.toString();
    }

    public static void makeSchemaLatest(SparkSession spark, List<StructField> paimonSchema, PaimonTableConf paimonTableConf) {
        List<String> colList = spark.sql("select col_name from schema_tbl").toJavaRDD().map( x -> x.getString(0)).map(String::trim).collect();
        System.out.println("current cols");
        for (String col : colList) {
            System.out.println(col);
        }
        for (StructField structField : paimonSchema) {
            if (colList.contains(structField.name())) continue;
            StringBuilder sb2 = new StringBuilder();
            sb2.append("alter table paimon.paimon_ods.").append(SchemaUtils.genTableName(paimonTableConf));
            sb2.append(" add column ").append(structField.name()).append(" ").append(structField.dataType().sql())
                    .append(" ").append("COMMENT '").append((String)structField.getComment().get())
                    .append("'");
            String alterSql = sb2.toString();
            spark.sql(alterSql);
        }
    }

    public static String genInitSql(SparkSession spark, List<StructField> paimonSchema,
                                    PaimonTableConf paimonTableConf, JdbcReadConf jdbcReadConf) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into paimon.paimon_ods.").append(genTableName(paimonTableConf)).append("(");
        sb.append(paimonSchema.stream().map(
                StructField::name
        ).collect(Collectors.joining(",\n\t")));
        sb.append(") select ");
        // sb.append(
        List<String> colList = paimonSchema.stream()
                .map(StructField::name)
                .filter(name -> !name.equals(DEFAULT_PARTITION_COLUMN_NAME))
                .filter(name -> !name.equals(MQ_PARTITION_INDEX_KEY))
                .filter(name -> !name.equals(MQ_PARTITION_OFFSET_KEY))
                .filter(name -> !name.equals(MQ_META_TIMESTAMP_KEY))
                .filter(name -> !name.equals(IS_CDC_DELETE_KEY))
                .collect(Collectors.toList());
        List<String> schemaCols =
                spark.sql("select col_name from schema_tbl").toJavaRDD().map(x -> x.getString(0)).map(String::trim)
                        .collect();
        if (jdbcReadConf.getHiveTableName() != null && !jdbcReadConf.getHiveTableName().isEmpty()) {
            spark.sql("select * from " + jdbcReadConf.getHiveTableName()).registerTempTable("schema_tbl2");
            schemaCols =
                    spark.sql("select col_name from schema_tbl2").toJavaRDD().map(x -> x.getString(0)).map(String::trim)
                            .collect();
        }
        System.out.println("generating initial sql");
        System.out.println("schema cols");
        for (String schemaCol : schemaCols) {
            System.out.println(schemaCol);
        }
        List<String> finalSchemaCols = schemaCols;
        sb.append(colList.stream().map(
                x -> finalSchemaCols.stream().anyMatch(col -> col.trim().equalsIgnoreCase(x)) ? x : "null as " + x
        ).collect(Collectors.joining(",\n\t")));
        sb.append(",\n");
        if (findPartitionColumn(paimonSchema, paimonTableConf).dataType() == DataTypes.TimestampNTZType) {
            sb.append("date(").append(findPartitionColumn(paimonSchema, paimonTableConf).name()).append(") as ")
                    .append(DEFAULT_PARTITION_COLUMN_NAME).append(",\n");
        }
        sb.append("0 as " + MQ_PARTITION_INDEX_KEY + ",\n");
        sb.append("0 as " + MQ_PARTITION_OFFSET_KEY + ",\n");
        sb.append(paimonTableConf.getMetaTimestampColumn()).append(" as ").append(MQ_META_TIMESTAMP_KEY).append(",\n");
        sb.append("0 as " + IS_CDC_DELETE_KEY + "\n");
        sb.append("from ods_tbl");
        return sb.toString();
    }
}
