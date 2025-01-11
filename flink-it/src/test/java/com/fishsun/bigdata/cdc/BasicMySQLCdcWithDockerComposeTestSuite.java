package com.fishsun.bigdata.cdc;

import com.fishsun.bigdata.PaimonBasicTestSuite;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;

import java.io.File;


public class BasicMySQLCdcWithDockerComposeTestSuite extends PaimonBasicTestSuite {
    protected static boolean USING_INTERNAL_DOCKER_COMPOSE = true;
    private DockerComposeContainer<?> composeContainer;
    public String jdbcUrl;
    private static final String SERVICE_NAME = "mysql";
    private static final int MYSQL_PORT = 3306;
    private static final String DBNAME = "inventory";
    private static final String JDBC_USER = "root";
    private static final String JDBC_PASS = "123456";
    public String tableName = "products";
    public String mappedHost;
    public int mappedPort;

    protected void initContainerWithDockerCompose() {
        logger.info("start init docker-compose");
        composeContainer = new DockerComposeContainer<>(new File("src/test/resources/docker-compose.yml"))
                .withExposedService(SERVICE_NAME, MYSQL_PORT, Wait.forListeningPort());
        logger.info("docker-compose initialization finished");
        logger.info("container is starting .......");
        composeContainer.start();
        logger.info("container started");
        mappedHost = composeContainer.getServiceHost(SERVICE_NAME, MYSQL_PORT);
        mappedPort = composeContainer.getServicePort(SERVICE_NAME, MYSQL_PORT);
        jdbcUrl = String.format("jdbc:mysql://%s:%d/" + DBNAME,
                mappedHost,
                mappedPort);
        System.out.println(jdbcUrl);
    }

    protected void init() {

    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        if (USING_INTERNAL_DOCKER_COMPOSE) {
            System.out.println("使用docker-compose初始化内部MySQL");
            initContainerWithDockerCompose();
        } else {
            System.out.println("使用外部docker-compose启动的MySQL");
            init();
        }
        registerCdcTable();
    }

    public void registerCdcTable() {
        tableEnv.executeSql("create database if not exists default_catalog.test");
        tableEnv.executeSql("CREATE TABLE default_catalog.test.products (\n" +
                " id INT NOT NULL,\n" +
                " name STRING,\n" +
                " description STRING,\n" +
                " weight DECIMAL(10,3),\n" +
                " PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'scan.startup.mode' = 'earliest-offset',\n" +
                " 'server-time-zone' = 'UTC',\n" +
                " 'server-id' = '5400-5404',\n" +
                " 'hostname' = '" + mappedHost + "',\n" +
                " 'port' = '" + mappedPort + "',\n" +
                " 'username' = '" + JDBC_USER + "',\n" +
                " 'password' = '" + JDBC_PASS + "',\n" +
                " 'database-name' = 'inventory',\n" +
                " 'table-name' = 'products'\n" +
                ")").print();
    }

    @Test
    public void testQueryFromCdcTable() {
        tableEnv.sqlQuery("select * from default_catalog.test.products")
                .execute()
                .print();
    }

    @Test
    public void testFlinkCdcIntegration() throws Exception {
//
        // 配置 MySQL CDC 源
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .startupOptions(StartupOptions.earliest()) // Start from earliest offset
                .hostname(mappedHost)
                .port(mappedPort)
                .databaseList(DBNAME) // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList(DBNAME + ".*") // set captured table
                .username(JDBC_USER)
                .password(JDBC_PASS)
                .serverId("5400-5404")
                .serverTimeZone("UTC")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

////
////
//        // 添加数据源
        DataStreamSource<String> source = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
//
        // 处理数据，可以添加转换操作
        source.print();
//
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
