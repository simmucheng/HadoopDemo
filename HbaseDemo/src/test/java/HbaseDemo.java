import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

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
        System.out.println(idvalue.toString());
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

    /**
     * 创建名字空间
     * @throws Exception
     */
    @Test
    public void createNameSpace() throws Exception {
        Configuration conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.0.110,192.168.0.113,192.168.0.111");
        conf.set("hbase.master", "192.168.0.110：60000");
        Connection conn=ConnectionFactory.createConnection(conf);
        Admin admiin=conn.getAdmin();
        NamespaceDescriptor namespaceDescriptor=NamespaceDescriptor.create("nns2").build();
        admiin.createNamespace(namespaceDescriptor);
        //列举所有的名字空间
        NamespaceDescriptor[] ns=admiin.listNamespaceDescriptors();
        for(NamespaceDescriptor it:ns){
            System.out.println(it.getName());
        }

    }

    /**
     * 列举所有的名字空间
     * @throws IOException
     */
    @Test
    public void listNameSpaces() throws IOException {
        Configuration conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.0.110,192.168.0.113,192.168.0.111");
        conf.set("hbase.master", "192.168.0.110：60000");
        Connection conn=ConnectionFactory.createConnection(conf);
        Admin admin=conn.getAdmin();
        NamespaceDescriptor[] ns = admin.listNamespaceDescriptors();
        for(NamespaceDescriptor it:ns){
            System.out.println(it.getName());
        }
    }

    /**
     * 创建一个表
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {
        Configuration conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.0.110,192.168.0.113,192.168.0.111");
        conf.set("hbase.master", "192.168.0.110：60000");
        Connection conn=ConnectionFactory.createConnection(conf);
        Admin admin=conn.getAdmin();
        TableName tableName=TableName.valueOf("nns2:t1");
        HTableDescriptor tdl=new HTableDescriptor(tableName);
        HColumnDescriptor col=new HColumnDescriptor("f1");
        tdl.addFamily(col);
        admin.createTable(tdl);
        System.out.println("over");
    }
    /**
     * 因为在删除表之前需要disable一个表
     * 所以这个步骤为disable表
     */
    @Test
    public void disableTable() throws IOException {
        Configuration conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.0.110,192.168.0.113,192.168.0.111");
        conf.set("hbase.master", "192.168.0.110：60000");
        Connection conn= ConnectionFactory.createConnection(conf);
        Admin admin=conn.getAdmin();
        //指明表所在的空间
        TableName tableName=TableName.valueOf("nns2:t1");
        admin.disableTable(tableName);
    }

    /**
     * 删除一个表
     * @throws IOException
     */
    @Test
    public void delateTable() throws IOException {
        Configuration conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.0.110,192.168.0.113,192.168.0.111");
        conf.set("hbase.master", "192.168.0.110：60000");
        Connection conn=ConnectionFactory.createConnection(conf);
        Admin admin=conn.getAdmin();
        TableName tableName=TableName.valueOf("nns2:t1");
        System.out.println();
        if(!admin.isTableDisabled(tableName)){
            admin.disableTable(tableName);
        }
        //删除表前需要将表disable
        admin.deleteTable(tableName);

    }
    /**
     * 删除表中rowkey数据
     */
    @Test
    public void delateData() throws IOException {
        Configuration conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.0.110,192.168.0.113,192.168.0.111");
        conf.set("hbase.master", "192.168.0.110：60000");
        Connection conn=ConnectionFactory.createConnection(conf);
        Admin admin=conn.getAdmin();
        TableName tableName=TableName.valueOf("nns1:t1");
        Table table=conn.getTable(tableName);
        Delete del=new Delete(Bytes.toBytes("row000002"));
        table.delete(del);
        System.out.println("yes");
    }

    /**
     * scan测试
     * 遍历数据
     */
    @Test
    public void scan() throws IOException {
        Configuration conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.0.110,192.168.0.113,192.168.0.111");
        conf.set("hbase.master", "192.168.0.110：60000");
        Connection conn=ConnectionFactory.createConnection(conf);
        Admin admin=conn.getAdmin();
        TableName tableName=TableName.valueOf("nns2:t2");
        Table table=conn.getTable(tableName);
        Scan scan=new Scan();
        scan.setStartRow(Bytes.toBytes("row0002"));
        scan.setStopRow(Bytes.toBytes("row0005"));
        ResultScanner res=table.getScanner(scan);
        Iterator<Result>it=res.iterator();
        while(it.hasNext()){
            Result kk=it.next();
            byte[] name=kk.getValue(Bytes.toBytes("f1"),Bytes.toBytes("name"));
            System.out.println(name.toString());
        }

    }
    @Test
    public void scan2(){

    }
}
