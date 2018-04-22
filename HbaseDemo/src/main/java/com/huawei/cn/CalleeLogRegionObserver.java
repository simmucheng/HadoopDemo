package com.huawei.cn;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;

//创建协处理器
public class CalleeLogRegionObserver extends BaseRegionObserver{

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);
        TableName calllogs=TableName.valueOf("calllogs");
        TableName tableName=e.getEnvironment().getRegion().getRegionInfo().getTable();
        //顾虑掉其他table
        if(!tableName.equals(tableName)){
            return ;
        }
        String rowkey= Bytes.toString(put.getRow());
        String[] arr=rowkey.split(",");
        //获得region number
        String hash=Util.getRegionNo(arr[4],arr[2]);
        String tmp_rowkey=hash+","+arr[4]+","+arr[2]+","+"1,"+arr[1]+","+arr[5];

        Put put1=new Put(Bytes.toBytes(tmp_rowkey));
        Table table=e.getEnvironment().getTable(tableName);
        table.put(put1);
    }
}
