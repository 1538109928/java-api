package Util;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HbaseUtil {

    Configuration conf = null;
    Connection conn = null;

    @Before
    public void getConfigAndConnection() {
        conf = HBaseConfiguration.create();
        //修改hosts文件
        // 10.86.33.61  hadoop1
        // 10.86.33.62  hadoop2
        // 10.86.33.63  hadoop3
        conf.set("hbase.zookeeper.quorum", "10.86.33.61,10.86.33.62,10.86.33.63");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    //创建表
    public void createTable() throws IOException {

        //账户类
        Admin admin = conn.getAdmin();
        if (admin.tableExists(TableName.valueOf("demo3"))) {
            System.out.println("Table exists!");
            return;
        }
        /* 已过时
        //表名
        HTableDescriptor tableDescriptor=new HTableDescriptor(TableName.valueOf("demo1"));
        //列族
        HColumnDescriptor columnDescriptor=new HColumnDescriptor("info1");
        //挂载列到表上
        tableDescriptor.addFamily(columnDescriptor);
        //客户端执行添加表
        admin.createTable(tableDescriptor);*/

        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf("demo3"));
        //列族
        builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of("info2"));

        //添加多个列族
        /*for (String columnFamily : columnFamilies) {
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily));
        }*/
        admin.createTable(builder.build());
    }

    @Test
    //查询所有表
    public void listTable() throws IOException {
        Admin admin=conn.getAdmin();
        //客户端执行list，获取表名数组
        TableName[] tableNames = admin.listTableNames();
        for (TableName name : tableNames) {
            System.out.println(name);
        }
    }

    //删除表
    @Test
    public void deleteTable() throws IOException {
        String tableName = "demo6";
        Admin admin=conn.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
    }

    @Test
    //插入数据
    public void insert() throws IOException {
        // 创建表对象
        Table table = conn.getTable(TableName.valueOf("demo3"));
        // 创建添加数据的行键对象
        Put put = new Put("row02".getBytes());
        // 设置行键数据
        put.addColumn("info1".getBytes(), "姓名".getBytes(), "张三".getBytes());
        // 添加数据
        table.put(put);
    }

    @After
    public void closeConn() throws IOException {
        conn.close();
    }
}

