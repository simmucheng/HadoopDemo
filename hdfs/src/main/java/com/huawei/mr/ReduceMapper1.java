package com.huawei.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReduceMapper1 extends Mapper<Text,IntWritable,Text,IntWritable>{
    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        if(value.get()>2){
            context.write(key,value);
        }
    }
}
