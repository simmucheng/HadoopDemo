package com.huawei.hdfs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HWMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text keyout=new Text();
        IntWritable valueout=new IntWritable();
        //以空格隔断
        String[] arr=value.toString().split(" ");//用空格分开
        for(String s: arr){
            keyout.set(s);
            valueout.set(1);
            context.write(keyout,valueout);
            context.getCounter("m",Util.getInfo(this,"")).increment(1);

        }

    }
}
