import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;

public class HbaseDemo {
    @Test
    public void get() throws Exception {
        Configuration conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.0.110,192.168.0.113,192.168.0.111");
        conf.set("hbase.master", "192.168.0.110：60000");
        Connection conn=ConnectionFactory.createConnection(conf);
        TableName tableName=TableName.valueOf("default:dd");
        Table table=conn.getTable(tableName);
        byte[] rowid=Bytes.toBytes("row1");
        Get get=new Get(rowid);
        Result r=table.get(get);
        byte[] idvalue=r.getValue(Bytes.toBytes("f1"),Bytes.toBytes("id"));
        System.out.println("----------"+idvalue.toString()+"----------");
        conn.close();
    }
    @Test
    public void put() throws Exception {
        Configuration conf= HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.0.110,192.168.0.113,192.168.0.111");
        conf.set("hbase.master", "192.168.0.110：60000");
        Connection conn= ConnectionFactory.createConnection(conf);
        TableName tableName=TableName.valueOf("nns1:t1");
        Table table=conn.getTable(tableName);
        byte[] rowid= Bytes.toBytes("row1");
        Put put=new Put(rowid);
        byte[] f1=Bytes.toBytes("f1");
        byte[] id=Bytes.toBytes("id");
        byte[] value=Bytes.toBytes("tom");
        put.addColumn(f1,id,value);
        table.put(put);
        table.close();
    }
    @Test
    public void biginsert() throws Exception {
        DecimalFormat format=new DecimalFormat("000000");
        Configuration conf= HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.0.110,192.168.0.113,192.168.0.111");
        conf.set("hbase.master", "192.168.0.110：60000");
        Connection conn= ConnectionFactory.createConnection(conf);
        TableName tableName=TableName.valueOf("nns1:t1");
        HTable table=(HTable) conn.getTable(tableName);
        //不要自动清理缓冲区
        table.setAutoFlush(false);
        for(int i=1;i<1000000;i++){
            Put put=new Put(Bytes.toBytes("row"+format.format(i)));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("id"),Bytes.toBytes("tom"+i));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes(" name"),Bytes.toBytes("tom"+i));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes(" name"),Bytes.toBytes("tom"+i));
            //操作一般会放在缓冲区，因为设置的不自动清理缓冲区
            table.put(put);
            if(i%2000==0){
                //执行缓冲区的操作
                table.flushCommits();
            }
        }
        //table.put(put);
    }
    @Test
    public void formatTest(){
        DecimalFormat format= new DecimalFormat();
        //format.applyPattern("0000000");
        format.applyPattern("###,###.00");
        //System.out.println(format.format(12));
        System.out.println(format.format(127444543345.2345));
    }
}
