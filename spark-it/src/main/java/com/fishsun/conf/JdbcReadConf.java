package com.fishsun.conf;

import lombok.*;

@Data
public class JdbcReadConf {
    // 不可变的属性
    private final String url;
    private final String driverClass;
    private final String username;
    private final String password;

    // 私有构造方法，通过Builder创建实例
    private JdbcReadConf(Builder builder) {
        this.url = builder.url;
        this.driverClass = builder.driverClass;
        this.username = builder.username;
        this.password = builder.password;

    }

    // 静态内部Builder类
    public static class Builder {
        private String url;
        private String driverClass;
        private String username;
        private String password;


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

        // 构建JdbcReadConf实例
        public JdbcReadConf build() {
            // 检查driverClass是否为空或空字符串，若是则根据url推断
            if (driverClass == null || driverClass.isEmpty()) {
                driverClass = inferDriverClass(url);
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
                return "com.mysql.jdbc.Driver";        // MySQL驱动
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
