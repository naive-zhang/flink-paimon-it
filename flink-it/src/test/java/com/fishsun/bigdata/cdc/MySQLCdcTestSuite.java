package com.fishsun.bigdata.cdc;

import com.fishsun.bigdata.PaimonBasicTestSuite;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.*;


public class MySQLCdcTestSuite extends PaimonBasicTestSuite {
    private static MySQLContainer<?> mysqlContainer;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        System.out.println("启动。。。。。。。。。。。。");
        // docker pull dockerproxy.net/debezium/example-mysql:1.1
        mysqlContainer = new MySQLContainer<>("debezium/example-mysql:1.1")
                .withDatabaseName("app_db")
                .withUsername("mysqluser")
                .withPassword("mysqlpw");

        System.out.println("配置完成");
        mysqlContainer.start();
        System.out.println("启动成功");

        try {
            mockInsertData();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void mockInsertData() throws SQLException {
        // 插入数据
        try (Connection conn = DriverManager.getConnection(
                mysqlContainer.getJdbcUrl(),
                mysqlContainer.getUsername(),
                mysqlContainer.getPassword());
             Statement stmt = conn.createStatement()) {
            // stmt.execute("create table test_table(id int, name varchar(255), PRIMARY KEY (id))");
            // stmt.execute("INSERT INTO test_table (id, name) VALUES (1, 'test')");
            // ResultSet resultSet = stmt.executeQuery("select * from test_table");
            // while (resultSet.next()) {
            //     System.out.println(resultSet.getString("id"));
            //     System.out.println(resultSet.getString("name"));
            // }
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

        }
    }

    @Test
    public void testFlinkCdcIntegration() throws Exception {

//        // 配置 MySQL CDC 源
//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname(mysqlContainer.getHost())
//                .port(mysqlContainer.getFirstMappedPort())
//                .databaseList(mysqlContainer) // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
//                .tableList("yourDatabaseName.yourTableName") // set captured table
//                .username("yourUsername")
//                .password("yourPassword")
//                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
//                .build();
//
//
//        // 添加数据源
//        DataStreamSource<String> source = env.addSource(mySQLSource);

        // 处理数据，可以添加转换操作
//        source.print();

        // 使用 Flink 的测试工具执行作业
        env.execute("Flink CDC Test");
    }

    @After
    public void tearDown() {
        if (mysqlContainer != null) {
            mysqlContainer.stop();
        }
    }
}
