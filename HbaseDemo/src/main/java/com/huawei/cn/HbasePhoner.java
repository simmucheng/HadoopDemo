package com.huawei.cn;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.SimpleFormatter;

public class HbasePhoner {

    public static void main(String[] args) throws IOException {
        Configuration conf= HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.0.110,192.168.0.113,192.168.0.111");
        conf.set("hbase.master", "192.168.0.110：60000");
        Connection conn= ConnectionFactory.createConnection(conf);
        SimpleDateFormat simpleDateFormat=new SimpleDateFormat();
        simpleDateFormat.applyPattern("yyyyMMddHHmmss");
        TableName tableName= TableName.valueOf("nns1:t1");
        Table table=conn.getTable(tableName);
        String callerId="13091907890";
        String calleeId="12390706789";
        String calltime=simpleDateFormat.format(new Date());
        int duration=100;
        DecimalFormat decimalFormat=new DecimalFormat();
        decimalFormat.applyPattern("00000");
        String durStr=decimalFormat.format(duration);

        //区域号
        int hash=(callerId+calltime.substring(0,6)).hashCode();
        hash=(hash&Integer.MAX_VALUE)%100;

        DecimalFormat df=new DecimalFormat();
        df.applyPattern("00");
        String regNo=df.format(hash);

        //拼接rowkey
        //其中0表示是主打
        String rowkey=regNo+","+callerId+","+calltime+","+"0,"+calleeId+","+durStr;
        Put put=new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("callerPos"),Bytes.toBytes("hubei"));
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("calleePos"),Bytes.toBytes("hunan"));

        table.put(put);
        System.out.println("over");
    }
}
