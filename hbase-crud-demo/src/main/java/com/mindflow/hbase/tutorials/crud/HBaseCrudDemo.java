package com.mindflow.hbase.tutorials.crud;

import com.mindflow.hbase.tutorials.util.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

/**
 * http://hbase.apache.org/1.2/apidocs/index.html
 *
 * @author Ricky Fung
 */
public class HBaseCrudDemo {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public static void main(String[] args) {

        new HBaseCrudDemo().scanTest();

        new HBaseCrudDemo().testCrud();
    }


    public void scanTest(){
        Connection connection = null;
        try {

            //创建表
            connection = HBaseConnectionUtils.getConnection();
            TableName tableName = TableName.valueOf("scan");

            createTable(connection, tableName, "c");

            //增加10条数据
            String rowKey = "00B99WJ42O2B3A:amazon_au:Covina:20171204";
            put(connection, tableName, rowKey, "c", "customer_num", "10");
            put(connection, tableName, rowKey, "c", "order_amount", "100");
            put(connection, tableName, rowKey, "c", "per_amount", "6.6");

            //cf2
            String rowKey1 = "00B99WJ42O2B3A:amazon_au:Covina:20171205";
            put(connection, tableName, rowKey1, "c", "customer_num", "101");
            put(connection, tableName, rowKey1, "c", "order_amount", "1001");
            put(connection, tableName, rowKey1, "c", "per_amount", "6.61");

            //cf2
            String rowKey3 = "00B99WJ42O2B3A:amazon_au:Covina:20171206";
            put(connection, tableName, rowKey3, "c", "customer_num", "1031");
            put(connection, tableName, rowKey3, "c", "order_amount", "1001");
            put(connection, tableName, rowKey3, "c", "per_amount", "6.61");


            //scan
            scan(connection, tableName);


        }
        catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

    }


    public void testCrud() {
        Connection connection = null;
        try {
            connection = HBaseConnectionUtils.getConnection();
            TableName tableName = TableName.valueOf("demo");

            //创建HBase表
            createTable(connection, tableName, "cf1", "cf2");

            //put
            String rowKey = "u12000";
            put(connection, tableName, rowKey, "cf1", "name", "ricky");
            put(connection, tableName, rowKey, "cf1", "password", "root");
            put(connection, tableName, rowKey, "cf1", "age", "28");

            //get
            get(connection, tableName, rowKey);

            //scan
            scan(connection, tableName);

            //coprosessor

            //delete
//            deleteTable(connection, tableName);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void scan(Connection connection, TableName tableName) throws IOException {
        Table table = null;
        try {
            table = connection.getTable(tableName);
            ResultScanner rs = null;
            try {
                //设定row的range
                Scan scan = new Scan(Bytes.toBytes("00B99WJ42O2B3A:amazon_au:Covina:20171204"), Bytes.toBytes("00B99WJ42O2B3A:amazon_au:Covina:20171207"));
                //设置要读取的列族
                scan.addFamily(Bytes.toBytes("c"));
                //设置要读取的列名
//                scan.addColumn(Bytes.toBytes("cf2"),Bytes.toBytes("age2"));
                Integer customerNum = 0;
                double orderVolume = 0d;
                rs = table.getScanner(scan);
                for(Result r:rs){

                    String num = Bytes.toString(r.getValue(Bytes.toBytes("c"),Bytes.toBytes("customer_num")));
                    String volume = Bytes.toString(r.getValue(Bytes.toBytes("c"),Bytes.toBytes("order_amount")));
                    if(StringUtils.isNotEmpty(num)){
                        customerNum = customerNum+Integer.valueOf(num);
                    }
                    if(StringUtils.isNotEmpty(volume)){
                        orderVolume = Integer.valueOf(volume) + orderVolume;
                    }

//                    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMap = r.getMap();
//                    for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : navigableMap.entrySet()){
//                        logger.info("row:{} key:{}", Bytes.toString(r.getRow()), Bytes.toString(entry.getKey()));
//                        NavigableMap<byte[], NavigableMap<Long, byte[]>> map =entry.getValue();
//                        for(Map.Entry<byte[], NavigableMap<Long, byte[]>> en:map.entrySet()){
//                            System.out.print(Bytes.toString(en.getKey())+"##");
//                            NavigableMap<Long, byte[]> ma = en.getValue();
//                            for(Map.Entry<Long, byte[]>e: ma.entrySet()){
//                                System.out.print(e.getKey()+"###");
//                                System.out.println(Bytes.toString(e.getValue()));
//                            }
//                        }
//                    }
                }
                System.out.println("_------->"+customerNum);
                System.out.println("------->"+orderVolume);
            } finally {
                if(rs!=null) {
                    rs.close();
                }
            }
        } finally {
            if(table!=null) {
                table.close();
            }
        }
    }

    //根据row key获取表中的该行数据
    public void get(Connection connection,TableName tableName,String rowKey) throws IOException {
        Table table = null;
        try {
            table = connection.getTable(tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMap = result.getMap();
            for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : navigableMap.entrySet()){

                logger.info("columnFamily:{}", Bytes.toString(entry.getKey()));
                NavigableMap<byte[], NavigableMap<Long, byte[]>> map =entry.getValue();
                for(Map.Entry<byte[], NavigableMap<Long, byte[]>> en:map.entrySet()){
                    System.out.print(Bytes.toString(en.getKey())+"##");
                    NavigableMap<Long, byte[]> nm = en.getValue();
                    for(Map.Entry<Long, byte[]> me : nm.entrySet()){
                        logger.info("column key:{}, value:{}", me.getKey(), me.getValue());
                    }
                }
            }
        } finally {
            if(table!=null) {
                table.close();
            }
        }
    }

    /**批量插入可以使用 Table.put(List<Put> list)**/
    public void put(Connection connection, TableName tableName,
                    String rowKey, String columnFamily, String column, String data) throws IOException {

        Table table = null;
        try {
            table = connection.getTable(tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
            table.put(put);
        } finally {
            if(table!=null) {
                table.close();
            }
        }
    }

    public void createTable(Connection connection, TableName tableName, String... columnFamilies) throws IOException {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            if(admin.tableExists(tableName)){
                logger.warn("table:{} exists!", tableName.getName());
            }else{
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                tableDescriptor.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation");
                for(String columnFamily : columnFamilies) {
                    tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
                }
                admin.createTable(tableDescriptor);
                logger.info("create table:{} success!", tableName.getName());
            }
        } finally {
            if(admin!=null) {
                admin.close();
            }
        }
    }

    //删除表中的数据
    public void deleteTable(Connection connection, TableName tableName) throws IOException {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            if (admin.tableExists(tableName)) {
                //必须先disable, 再delete
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
        } finally {
            if(admin!=null) {
                admin.close();
            }
        }
    }

    public void disableTable(Connection connection, TableName tableName) throws IOException {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            if(admin.tableExists(tableName)){
                admin.disableTable(tableName);
            }
        } finally {
            if(admin!=null) {
                admin.close();
            }
        }
    }
}
