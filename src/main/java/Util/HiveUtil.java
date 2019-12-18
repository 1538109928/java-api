package Util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class HiveUtil {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";  // 此Class 位于 hive-jdbc的jar包下//org.apache.hive.jdbc.HiveDriver
    private static String url = "jdbc:hive2://hadoop1:10000/default";  //hiveserver2使用hive2，10000为默认端口
    private static String user = "root";  //在hive-site中配置，并在hadoop中core-site.xml配置给予root权限
    private static String password = "123456";

    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;

    // 加载驱动、创建连接
    @Before
    public void init() throws Exception {
        Class.forName(driverName);
        conn = DriverManager.getConnection(url,user,password);
        if(conn==null) {
            System.out.println("连接失败");
        } else {
            stmt = conn.createStatement();
        }
    }

    // 创建数据库
    @Test
    public void createDatabase() throws Exception {
        String sql = "create database hive_jdbc_test";
        stmt.execute(sql);
        System.out.println("成功创建数据库");
    }

    // 创建表
    @Test
    public void createTable() throws Exception {
        String sql = "create table pokes (foo int, bar string) row format delimited fields terminated by ','";
        stmt.execute(sql);
    }

    // 查询所有表
    @Test
    public void showTables() throws Exception {
        String sql = "show tables";
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    // 查看表结构
    @Test
    public void descTable() throws Exception {
        String sql = "desc pokes";
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1) + "\t" + rs.getString(2));
        }
    }

    // 查询数据
    @Test
    public void selectData() throws Exception {
        String sql = "select * from pokes";
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString("foo") + "\t\t" + rs.getString("bar"));
        }
    }

    // 统计查询（会运行mapreduce作业）
    @Test
    public static void countData() throws Exception {
        String sql = "select count(1) from pokes";
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getInt(1) );
        }
    }

    // 删除数据库表
    @Test
    public void dropTable() throws Exception {
        String sql = "drop table if exists pokes";
        stmt.execute(sql);
    }

    @Test
    // 删除数据库
    public void dropDatabase() throws Exception {
        String sql = "drop database if exists test_db";
        stmt.execute(sql);
    }

    // 释放资源
    @After
    public void destory() throws Exception {
        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
