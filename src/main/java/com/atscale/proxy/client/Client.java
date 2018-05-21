package com.atscale.proxy.client;
//package com.atscale.engine.examples.helloworld;

import java.sql.*;
import java.util.Properties;

/**
 * Simple example of using the AtScale Engine via JDBC.
 * Example: $ mvn clean compile exec:java -Dexec.mainClass="com.atscale.engine.examples.helloworld.HelloWorld" -Dexec.args="local.infra.atscale.com test_export"
 * Reference: https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients
 */
public class Client {

  private static String SQL_QUERY = "SELECT column2, SUM(testvalues) AS qt_hub91yccnb FROM connor AS t0 GROUP BY column2 ORDER BY qt_hub91yccnb DESC";

  public static void main(String args[]) throws SQLException, ClassNotFoundException, InterruptedException {
    Class.forName("org.postgresql.Driver");
    
    String url = "jdbc:postgresql://localhost:5433";
    Properties props = new Properties();
    props.setProperty("user","connor");
    props.setProperty("password","");
    Connection con = DriverManager.getConnection(url, props);
    
    //System.out.println("About to connect to: " + connectionUrl);
    //Connection connection = DriverManager.getConnection(connectionUrl, "admin", "admin");
    
    Statement statement = con.createStatement();
    ResultSet rs = null;

    System.out.println("\n\nAbout to execute query:\n\n" + SQL_QUERY);
    rs = statement.executeQuery(SQL_QUERY); 
    System.out.println("\n\nQuery results:\n");
    while (rs.next()) {
      System.out.println(rs.getString(1) + " " + rs.getString(2));
    }

    /*String createTableName = args[1] + "_" + new java.util.Date().getTime();
    String sqlExport = String.format(SQL_EXPORT_TEMPLATE, createTableName, SQL_QUERY);
    System.out.println("\n\nAbout to execute export:\n\n" + sqlExport);

    resultSet = statement.executeQuery(sqlExport);
    resultSet.next();
    System.out.println("\n\nTable created!");
    */
  }
}