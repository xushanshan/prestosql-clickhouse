package com.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ClickhouseJdbcConfigTest {
    public static void main(String[] args) throws Exception {
//        Properties properties = ClickhouseJdbcUrlParser.parse("jdbc:clickhouse://172.20.216.156:8123", new Properties()).asProperties();
//        Properties properties = ClickhouseJdbcUrlParser.parse("jdbc:clickhouse://172.20.216.156:8123/Insight_datafarm_test_wei", new Properties()).asProperties();
//        String database = properties.getProperty(ClickHouseQueryParam.DATABASE.getKey());

        Connection connection = DriverManager.getConnection("jdbc:clickhouse://localhost:8123");

        ResultSet resultSet = connection.getMetaData().getSchemas();
        while (resultSet.next()) {
            System.out.println(resultSet.getString("TABLE_SCHEM") + " - "+ resultSet.getString("TABLE_CATALOG"));
        }


        System.out.println("===================================================");
        Statement statement = connection.createStatement();
        resultSet = statement.executeQuery("SHOW DATABASES");
        while (resultSet.next()) {
            System.out.println(resultSet.getString("name"));
        }


    }
}
