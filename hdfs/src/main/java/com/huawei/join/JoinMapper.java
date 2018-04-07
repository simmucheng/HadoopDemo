package com.huawei.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

public class JoinMapper extends Mapper<LongWritable,Text,Text,NullWritable>{

    Map<String,String>customsMap=new HashMap<String, String>();
    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf= context.getConfiguration();
        FileSystem fs= FileSystem.get(conf);
        FSDataInputStream fis=fs.open(new Path("/Users/simmucheng/tmp/join_test/customer.txt"));
        BufferedReader br=new BufferedReader(new InputStreamReader(fis));
        String line=null;
        while((line=br.readLine())!=null){
            String cid =line.substring(0,line.indexOf(","));
            customsMap.put(cid,line);
        }

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       String str=value.toString();
       String id=str.substring(str.lastIndexOf(",")+1);
       String productInfo=str.substring(0,str.lastIndexOf(','));
       String customerinfo=customsMap.get(id);
       context.write(new Text(customerinfo+','+productInfo),NullWritable.get());
    }
}
