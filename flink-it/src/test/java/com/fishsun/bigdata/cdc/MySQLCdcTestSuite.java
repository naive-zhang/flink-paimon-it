package com.fishsun.bigdata.cdc;

import com.fishsun.bigdata.PaimonBasicTestSuite;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class MySQLCdcTestSuite extends PaimonBasicTestSuite {
    public DockerComposeContainer<?> composeContainer;
    public String jdbcUrl;
    public String jdbcUser = "root";
    public String jdbcPass = "123456";

    @Override
    @Before
    public void setUp() {
        super.setUp();
        System.out.println("启动。。。。。。。。。。。。");
        composeContainer = new DockerComposeContainer<>(new File("src/test/resources/docker-compose.yml"))
                .withExposedService("mysql", 3306
                        , Wait.forListeningPort()
                )
        // .withLocalCompose(true)
        ;
        System.out.println("配置完成");
        composeContainer.start();
        System.out.println("启动成功");
        jdbcUrl = String.format("jdbc:mysql://%s:%d",
                composeContainer.getServiceHost("mysql", 3306),
                composeContainer.getServicePort("mysql", 3306));

        try {
            mockInsertData();
            System.out.println(jdbcUrl);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void mockInsertData() throws SQLException {
        // 插入数据
        try (Connection conn = DriverManager.getConnection(
                jdbcUrl,
                jdbcUser,
                jdbcPass);
             Statement stmt = conn.createStatement()) {
            // stmt.execute("create table test_table(id int, name varchar(255), PRIMARY KEY (id))");
            // stmt.execute("INSERT INTO test_table (id, name) VALUES (1, 'test')");
            // ResultSet resultSet = stmt.executeQuery("select * from test_table");
            // while (resultSet.next()) {
            //     System.out.println(resultSet.getString("id"));
            //     System.out.println(resultSet.getString("name"));
            // }
            stmt.execute("create database if not exists app_db");
            stmt.execute("use app_db");
            stmt.execute("CREATE TABLE `orders` (\n" +
                    "`id` INT NOT NULL,\n" +
                    "`price` DECIMAL(10,2) NOT NULL,\n" +
                    "PRIMARY KEY (`id`)\n" +
                    ")");
            stmt.execute("INSERT INTO `orders` (`id`, `price`) VALUES (1, 4.00)");
            stmt.execute("INSERT INTO `orders` (`id`, `price`) VALUES (2, 100.00)");
            stmt.execute("CREATE TABLE `shipments` (\n" +
                    "`id` INT NOT NULL,\n" +
                    "`city` VARCHAR(255) NOT NULL,\n" +
                    "PRIMARY KEY (`id`)\n" +
                    ")");
            stmt.execute("INSERT INTO `shipments` (`id`, `city`) VALUES (1, 'beijing')");
            stmt.execute("INSERT INTO `shipments` (`id`, `city`) VALUES (2, 'xian')");
            stmt.execute("CREATE TABLE `products` (\n" +
                    "`id` INT NOT NULL,\n" +
                    "`product` VARCHAR(255) NOT NULL,\n" +
                    "PRIMARY KEY (`id`)\n" +
                    ")");
            stmt.execute("INSERT INTO `products` (`id`, `product`) VALUES (1, 'Beer')");
            stmt.execute("INSERT INTO `products` (`id`, `product`) VALUES (2, 'Cap')");
            stmt.execute("INSERT INTO `products` (`id`, `product`) VALUES (3, 'Peanut')");
            ResultSet resultSet = stmt.executeQuery("select id, product from products");
            while (resultSet.next()) {
                System.out.println(resultSet.getString("product"));
            }
            jdbcUrl = jdbcUrl + "/app_test";
        }
    }

    @Test
    public void testFlinkCdcIntegration() throws Exception {

        // 配置 MySQL CDC 源
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .startupOptions(StartupOptions.earliest()) // Start from earliest offset
                .hostname(composeContainer.getServiceHost("mysql", 3306))
                .port(composeContainer.getServicePort("mysql", 3306))
                .databaseList(
                        "app_test") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("orders") // set captured table
                .username(jdbcUser)
                .password(jdbcPass)
                .serverId("5400-5404")
                .serverTimeZone("UTC")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
//
//
//        // 添加数据源
        DataStreamSource<String> source = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        // 处理数据，可以添加转换操作
        source.print();

        // 使用 Flink 的测试工具执行作业
        // env.execute("Flink CDC Test");
        env.execute();
    }

    @After
    public void tearDown() {
        if (composeContainer != null) {
            composeContainer.stop();
        }
    }
}
