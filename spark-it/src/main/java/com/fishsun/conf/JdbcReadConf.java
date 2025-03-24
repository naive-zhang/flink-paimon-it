package com.fishsun.conf;

import lombok.*;

import java.util.ArrayList;
import java.util.List;

@Data
public class JdbcReadConf {
    // 不可变的属性
    private final String url;
    private final String driverClass;
    private final String username;
    private final String password;

    private final String partitionColumn;
    private final String lowerBound;
    private final String upperBound;
    private final String query;
    private final String dbTable;
    private boolean isUseQuery;
    private boolean isUsePartitionColumn;
    private final Boolean isPartitionTable;
    private final String partitionFromColumn;
    private final List<String> primaryKeys;

    // 私有构造方法，通过Builder创建实例
    private JdbcReadConf(Builder builder) {
        this.url = builder.url;
        this.driverClass = builder.driverClass;
        this.username = builder.username;
        this.password = builder.password;
        this.partitionColumn = builder.partitionColumn;
        this.lowerBound = builder.lowerBound;
        this.upperBound = builder.upperBound;
        this.query = builder.query;
        this.dbTable = builder.dbTable;
        this.isPartitionTable = builder.isPartitionTable;
        if (this.query != null) {
            this.isUseQuery = true;
        }
        if (this.query == null && this.partitionColumn != null) {
            this.isUsePartitionColumn = true;
        }
        this.partitionFromColumn = builder.partitionFromColumn;
        this.primaryKeys = builder.primaryKeys;
    }

    // 静态内部Builder类
    public static class Builder {
        private String url;
        private String driverClass;
        private String username;
        private String password;
        private String partitionColumn;
        private String lowerBound;
        private String upperBound;
        private String query;
        private String dbTable;
        private Boolean isPartitionTable;
        private String partitionFromColumn;
        private List<String> primaryKeys;

        public Builder primaryKeys(List<String> primaryKeys) {
            this.primaryKeys = primaryKeys;
            return this;
        }


        // 设置url的方法，返回Builder自身以支持链式调用
        public Builder url(String url) {
            this.url = url;
            return this;
        }

        // 设置driverClass的方法，返回Builder自身以支持链式调用
        public Builder driverClass(String driverClass) {
            this.driverClass = driverClass;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder partitionColumn(String partitionColumn) {
            this.partitionColumn = partitionColumn;
            return this;
        }

        public Builder lowerBound(String lowerBound) {
            this.lowerBound = lowerBound;
            return this;
        }

        public Builder upperBound(String upperBound) {
            this.upperBound = upperBound;
            return this;
        }

        public Builder query(String query) {
            this.query = query;
            return this;
        }

        public Builder dbTable(String dbTable) {
            this.dbTable = dbTable;
            return this;
        }

        public Builder isPartitionTable(boolean isPartitionTable) {
            this.isPartitionTable = isPartitionTable;
            return this;
        }

        public Builder partitionFromColumn(String partitionFromColumn) {
            this.partitionFromColumn = partitionFromColumn;
            return this;
        }

        // 构建JdbcReadConf实例
        public JdbcReadConf build() {
            // 检查driverClass是否为空或空字符串，若是则根据url推断
            if (driverClass == null || driverClass.isEmpty()) {
                driverClass = inferDriverClass(url);
            }
            if (isPartitionTable == null) {
                isPartitionTable = false;
            }
            if (primaryKeys == null) {
                this.primaryKeys = new ArrayList<>();
            }
            return new JdbcReadConf(this);
        }

        // 根据url推断driverClass的私有方法
        private String inferDriverClass(String url) {
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

    public static void main(String[] args) {

    }
}
